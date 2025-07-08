package mapreduce;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MapReduceMultiNodes {
    private static final int PORT = 5000;
    private static final String WORKERS_FILE = "workers.txt";

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java MapReduceMultiNodes <input-file> <master-ip>");
            System.exit(1);
        }
        String inputFile = args[0];
        String masterIp  = args[1];

        List<String> workersList = Files.readAllLines(Paths.get(WORKERS_FILE));
        String localIp = InetAddress.getLocalHost().getHostAddress();

        if (localIp.equals(masterIp)) {
            System.out.println("master");
            runAsMaster(inputFile, workersList);
        } else {
            System.out.println("worker");
            runAsWorker(masterIp);
        }
    }

    private static void runAsMaster(String inputFile, List<String> workersList) throws Exception {
        List<String> allLines = Files.readAllLines(Paths.get(inputFile));
        int numWorkers = workersList.size() + 1;
        int chunkSize = allLines.size() / numWorkers;

        List<String> masterChunk = allLines.subList(0, chunkSize);
        ChunkWordFrequencyThread masterTask = new ChunkWordFrequencyThread(masterChunk, 0, numWorkers);
        Thread masterThread = new Thread(masterTask);
        masterThread.start();

        for (int i = 0; i < workersList.size(); i++) {
            String workerIp = workersList.get(i);
            try (Socket sock = new Socket(workerIp, PORT);
                 ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(sock.getInputStream())) {
                out.writeUTF("MAP");
                List<String> chunk = allLines.subList((i + 1) * chunkSize,
                        (i + 2 == numWorkers) ? allLines.size() : (i + 2) * chunkSize);
                out.writeObject(chunk);
                out.flush();
                in.readUTF();
            }
        }
        masterThread.join();
        System.out.println("MAP FINISHED");

        List<String> allHosts = new ArrayList<>();
        allHosts.add(InetAddress.getLocalHost().getHostAddress());
        allHosts.addAll(workersList);
        for (String host : workersList) {
            try (Socket sock = new Socket(host, PORT);
                 ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(sock.getInputStream())) {
                out.writeUTF("SHUFFLE");
                out.writeObject(allHosts);
                out.flush();
                in.readUTF();
            }
        }
        System.out.println("SHUFFLE FINISHED");

        Map<String, Integer> finalFreq = new HashMap<>();
        masterTask.prepareShuffle();
        masterTask.setWorkersRef(createLocalWorkersRef(allHosts));
        masterTask.executeShuffle();
        masterTask.getReceivedData().forEach((k, v) -> finalFreq.merge(k, v, Integer::sum));

        for (String host : workersList) {
            try (Socket sock = new Socket(host, PORT);
                 ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(sock.getInputStream())) {
                out.writeUTF("RESULT");
                out.flush();
                @SuppressWarnings("unchecked")
                Map<String, Integer> part = (Map<String, Integer>) in.readObject();
                part.forEach((k, v) -> finalFreq.merge(k, v, Integer::sum));
            }
        }

        StringBuilder sb = new StringBuilder();
        finalFreq.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .limit(20)
                .forEach(e -> sb.append(e.getKey()).append(": ").append(e.getValue()).append("\n"));
        System.out.print(sb.toString());
    }

    private static ChunkWordFrequencyThread[] createLocalWorkersRef(List<String> hosts) {
        int n = hosts.size();
        ChunkWordFrequencyThread[] refs = new ChunkWordFrequencyThread[n];
        refs[0] = null;
        return refs;
    }

    private static void runAsWorker(String masterIp) throws Exception {
        Map<String, Integer> localFreq = new HashMap<>();
        Map<String, Integer> receivedData = new ConcurrentHashMap<>();

        try (ServerSocket server = new ServerSocket(PORT)) {
            while (true) {
                try (Socket sock = server.accept();
                     ObjectInputStream in = new ObjectInputStream(sock.getInputStream());
                     ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream())) {
                    String cmd = in.readUTF();
                    if ("MAP".equals(cmd)) {
                        @SuppressWarnings("unchecked")
                        List<String> chunk = (List<String>) in.readObject();
                        for (String line : chunk) {
                            for (String token : line
                                    .toLowerCase()
                                    .replaceAll("[^a-z0-9\\s]", "")
                                    .split("\\s+")) {
                                if (!token.isEmpty())
                                    localFreq.merge(token, 1, Integer::sum);
                            }
                        }
                        out.writeUTF("ACK_MAP");
                        out.flush();
                    } else if ("SHUFFLE".equals(cmd)) {
                        @SuppressWarnings("unchecked")
                        List<String> hosts = (List<String>) in.readObject();
                        int numHosts = hosts.size();
                        Map<Integer, Map<String, Integer>> buckets = new HashMap<>();
                        for (Map.Entry<String, Integer> e : localFreq.entrySet()) {
                            int dest = (e.getKey().hashCode() & Integer.MAX_VALUE) % numHosts;
                            buckets.computeIfAbsent(dest, k -> new HashMap<>()).put(e.getKey(), e.getValue());
                        }
                        for (int i = 0; i < hosts.size(); i++) {
                            String host = hosts.get(i);
                            try (Socket s2 = new Socket(host, PORT);
                                 ObjectOutputStream o2 = new ObjectOutputStream(s2.getOutputStream());
                                 ObjectInputStream i2 = new ObjectInputStream(s2.getInputStream())) {
                                o2.writeUTF("SHUFFLE_BUCKET");
                                o2.writeObject(buckets.getOrDefault(i, Collections.emptyMap()));
                                o2.flush();
                                i2.readUTF();
                            }
                        }
                        out.writeUTF("ACK_SHUFFLE");
                        out.flush();
                    } else if ("SHUFFLE_BUCKET".equals(cmd)) {
                        @SuppressWarnings("unchecked")
                        Map<String, Integer> bucket = (Map<String, Integer>) in.readObject();
                        bucket.forEach((k, v) -> receivedData.merge(k, v, Integer::sum));
                        out.writeUTF("ACK_BUCKET");
                        out.flush();
                    } else if ("RESULT".equals(cmd)) {
                        out.writeObject(receivedData);
                        out.flush();
                    }
                } catch (IOException | ClassNotFoundException e) {
                    System.err.println("Error handling connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Could not start server on port " + PORT + ": " + e.getMessage());
        }
    }
}
