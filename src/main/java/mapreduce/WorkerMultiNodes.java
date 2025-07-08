package mapreduce;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class WorkerMultiNodes {
    private final String masterHost;
    private final int masterPort;
    private final int listenPort;
    private final Map<String, Integer> localMap = new HashMap<>();
    private final Map<String, List<Integer>> partitions = new ConcurrentHashMap<>();
    private final List<InetSocketAddress> workerAddrs = new ArrayList<>();
    private int myIndex = -1;
    private ServerSocket peerServer;  // Serveur de shuffle

    public WorkerMultiNodes(String masterHost, int masterPort, int listenPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.listenPort = listenPort;
    }

    /**
     * Démarre le serveur de partitions, charge workers.txt, se connecte au Master.
     */
    public void start() {
        // Serveur pour recevoir les partitions lors du shuffle
        try {
            peerServer = new ServerSocket(listenPort);
            System.out.println("Worker shuffle server listening on port " + listenPort);
        } catch (IOException e) {
            System.err.println("Cannot start shuffle server on port " + listenPort + ": " + e.getMessage());
            return;
        }

        // Thread d'accept pour le shuffle
        new Thread(() -> {
            while (!peerServer.isClosed()) {
                try {
                    Socket s = peerServer.accept();
                    receivePartition(s);
                } catch (IOException e) {
                    System.err.println("Shuffle accept error: " + e.getMessage());
                    break;
                }
            }
        }).start();

        // Charge la liste des Workers depuis workers.txt
        try {
            List<String> lines = Files.readAllLines(Paths.get("workers.txt"));
            for (int i = 0; i < lines.size(); i++) {
                String[] hp = lines.get(i).split(":");
                workerAddrs.add(new InetSocketAddress(hp[0], Integer.parseInt(hp[1])));
                if (Integer.parseInt(hp[1]) == listenPort) {
                    myIndex = i;
                }
            }
        } catch (IOException e) {
            System.err.println("Cannot read workers.txt: " + e.getMessage());
            closePeerServer();
            return;
        }

        // Connexion au Master
        try (Socket sock = new Socket(masterHost, masterPort);
             BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
             PrintWriter out = new PrintWriter(sock.getOutputStream(), true)) {
            out.println("REGISTER " + listenPort);
            String req;
            while ((req = in.readLine()) != null) {
                String[] parts = req.split(" ");
                switch (parts[0]) {
                    case "MAP":
                        map(parts[1], out);
                        break;
                    case "SHUFFLE":
                        shuffle(out);
                        break;
                    case "REDUCE":
                        reduce(out);
                        break;
                }
            }
        } catch (IOException e) {
            System.err.println("Master connection error: " + e.getMessage());
        } finally {
            closePeerServer();
        }
    }

    private void map(String filepath, PrintWriter out) {
        try {
            byte[] bytes = Files.readAllBytes(Paths.get(filepath));
            for (String word : new String(bytes).split("\\s+")) {
                localMap.merge(word, 1, Integer::sum);
            }
        } catch (IOException e) {
            System.err.println("Map read error on " + filepath + ": " + e.getMessage());
        }
        out.println("SIGNAL:MAP_DONE");
    }

    private void shuffle(PrintWriter out) {
        localMap.forEach((k, c) -> {
            int t = Math.floorMod(k.hashCode(), workerAddrs.size());
            if (t == myIndex) {
                partitions.computeIfAbsent(k, kk -> new ArrayList<>()).add(c);
            } else {
                InetSocketAddress a = workerAddrs.get(t);
                try (Socket s = new Socket(a.getHostString(), a.getPort());
                     PrintWriter pw = new PrintWriter(s.getOutputStream(), true)) {
                    pw.println(k + ":" + c);
                } catch (IOException e) {
                    System.err.println("Shuffle send error to " + a + ": " + e.getMessage());
                }
            }
        });
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {}
        out.println("SIGNAL:SHUFFLE_DONE");
    }

    private void receivePartition(Socket s) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
            String l;
            while ((l = br.readLine()) != null) {
                String[] kv = l.split(":");
                partitions.computeIfAbsent(kv[0], xx -> new ArrayList<>()).add(Integer.parseInt(kv[1]));
            }
        } catch (IOException e) {
            System.err.println("Shuffle receive error: " + e.getMessage());
        }
    }

    private void reduce(PrintWriter out) {
        partitions.forEach((k, list) ->
                System.out.println(k + " -> " + list.stream().mapToInt(i -> i).sum())
        );
        out.println("SIGNAL:REDUCE_DONE");
    }

    private void closePeerServer() {
        try {
            if (peerServer != null && !peerServer.isClosed()) {
                peerServer.close();
            }
        } catch (IOException ignored) {}
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java WorkerMultiNodes <masterHost> <masterPort> <listenPort>");
            return;
        }
        new WorkerMultiNodes(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2])).start();
    }
}