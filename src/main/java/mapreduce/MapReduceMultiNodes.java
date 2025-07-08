package mapreduce;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

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

        // Read list of worker IPs from workers.txt at project root
        List<String> workersList = Files.readAllLines(Paths.get(WORKERS_FILE));
        int numMachines = workersList.size() + 1; // include master

        String localIp = InetAddress.getLocalHost().getHostAddress();
        if (localIp.equals(masterIp)) {
            runAsMaster(inputFile, workersList);
        } else {
            runAsWorker(masterIp);
        }
    }

    private static void runAsMaster(String inputFile, List<String> workersList) throws Exception {
        // 1. Read input
        List<String> allLines = Files.readAllLines(Paths.get(inputFile));
        int numWorkers = workersList.size() + 1;
        int chunkSize = allLines.size() / numWorkers;

        // 2. Launch map on master chunk
        List<String> masterChunk = allLines.subList(0, chunkSize);
        ChunkWordFrequencyThread masterTask = new ChunkWordFrequencyThread(masterChunk, 0, numWorkers);
        Thread masterThread = new Thread(masterTask);
        masterThread.start();

        // 3. Send chunks to workers via TCP
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
                in.readUTF(); // ACK_MAP
            }
        }
        masterThread.join();
        System.out.println("MAP FINISHED");

        // 4. Notify workers for shuffle
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
                in.readUTF(); // ACK_SHUFFLE
            }
        }
        System.out.println("SHUFFLE FINISHED");

        // 5. Collect reduced data from master and workers
        Map<String, Integer> finalFreq = new HashMap<>();
        // Master local shuffle
        masterTask.prepareShuffle();
        masterTask.setWorkersRef(createLocalWorkersRef(workersList.size() + 1));
        masterTask.executeShuffle();
        masterTask.getReceivedData().forEach((k, v) -> finalFreq.merge(k, v, Integer::sum));

        // Collect from each worker
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

        // 6. Print top 20 words by frequency
        StringBuilder sb = new StringBuilder();
        finalFreq.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .limit(20)
                .forEach(e -> sb.append(e.getKey()).append(": ").append(e.getValue()).append("\n"));
        System.out.print(sb.toString());
    }

    private static ChunkWordFrequencyThread[] createLocalWorkersRef(int total) {
        // Placeholder: in a real setup, we'd hold references to remote stubs
        return new ChunkWordFrequencyThread[total];
    }

    private static void runAsWorker(String masterIp) throws Exception {
        try (ServerSocket server = new ServerSocket(PORT)) {
            while (true) {
                try (Socket sock = server.accept();
                     ObjectInputStream in = new ObjectInputStream(sock.getInputStream());
                     ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream())) {
                    String cmd = in.readUTF();
                    if ("MAP".equals(cmd)) {
                        @SuppressWarnings("unchecked")
                        List<String> chunk = (List<String>) in.readObject();
                        ChunkWordFrequencyThread task = new ChunkWordFrequencyThread(chunk, 0, 0);
                        task.run();
                        out.writeUTF("ACK_MAP");
                        out.flush();
                    } else if ("SHUFFLE".equals(cmd)) {
                        @SuppressWarnings("unchecked")
                        List<String> hosts = (List<String>) in.readObject();
                        // TODO: implement shuffle partitioning and communication
                        out.writeUTF("ACK_SHUFFLE");
                        out.flush();
                    } else if ("RESULT".equals(cmd)) {
                        Map<String, Integer> result = new HashMap<>();
                        // TODO: return the local reduced data after shuffle
                        out.writeObject(result);
                        out.flush();
                    }
                } catch (IOException | ClassNotFoundException e) {
                    System.err.println("Error handling worker connection: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            System.err.println("Could not start worker server on port " + PORT + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}