package mapreduce;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class MasterMultiNodes {
    private final int port;
    private final List<WorkerHandler> workers = new ArrayList<>();
    private final ExecutorService exec = Executors.newCachedThreadPool();

    public MasterMultiNodes(int port) {
        this.port = port;
    }

    /**
     * Démarre le serveur Master et accepte les connexions des Workers.
     */
    public void start() {
        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("Master listening on port " + port);
            exec.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Socket sock = server.accept();
                        WorkerHandler handler = new WorkerHandler(sock);
                        workers.add(handler);
                        exec.submit(handler);
                    } catch (IOException e) {
                        System.err.println("Error accepting worker connection: " + e.getMessage());
                    }
                }
            });
        } catch (IOException e) {
            System.err.println("Failed to start Master on port " + port + ": " + e.getMessage());
        }
    }

    /**
     * Exécute les phases Map, Shuffle, Reduce en séquence.
     */
    public void runPipeline() throws InterruptedException {
        List<String> splits;
        // Chargement des splits depuis le dossier texts/
        try (Stream<Path> paths = Files.list(Paths.get("./texts"))) {
            splits = paths
                    .filter(p -> p.toString().endsWith(".wet"))
                    .map(p -> p.getFileName().toString())
                    .toList();
        } catch (IOException e) {
            System.err.println("Error reading splits: " + e.getMessage());
            return;
        }

        Thread.sleep(2000); // laisser le temps aux Workers de se connecter

        long t0 = System.currentTimeMillis();
        broadcast("MAP", splits);
        waitForPhase("MAP_DONE");
        long mapTime = System.currentTimeMillis() - t0;
        System.out.println("MAP FINISHED in " + mapTime + " ms");

        long t1 = System.currentTimeMillis();
        broadcast("SHUFFLE", Collections.emptyList());
        waitForPhase("SHUFFLE_DONE");
        long shuffleTime = System.currentTimeMillis() - t1;
        System.out.println("SHUFFLE FINISHED in " + shuffleTime + " ms");

        long t2 = System.currentTimeMillis();
        broadcast("REDUCE", Collections.emptyList());
        waitForPhase("REDUCE_DONE");
        long reduceTime = System.currentTimeMillis() - t2;
        System.out.println("REDUCE FINISHED in " + reduceTime + " ms");

        System.out.println("Longest phase: " + longest(mapTime, shuffleTime, reduceTime));
        System.out.println("Fastest phase: " + fastest(mapTime, shuffleTime, reduceTime));
        exec.shutdown();
    }

    private void broadcast(String cmd, List<String> args) {
        for (WorkerHandler w : workers) {
            w.sendCommand(cmd, args);
        }
    }

    private void waitForPhase(String doneSignal) throws InterruptedException {
        for (WorkerHandler w : workers) w.waitFor(doneSignal);
    }

    private String longest(long m, long s, long r) {
        return (m>=s && m>=r)?"MAP":(s>=m&&s>=r?"SHUFFLE":"REDUCE");
    }

    private String fastest(long m, long s, long r) {
        return (m<=s && m<=r)?"MAP":(s<=m&&s<=r?"SHUFFLE":"REDUCE");
    }

    public static void main(String[] args) throws Exception {
        int port = args.length>0?Integer.parseInt(args[0]):5000;
        MasterMultiNodes master = new MasterMultiNodes(port);
        master.start();
        master.runPipeline();
    }

    private static class WorkerHandler implements Runnable {
        private final Socket sock;
        private final BufferedReader in;
        private final PrintWriter out;
        private final BlockingQueue<String> signals = new LinkedBlockingQueue<>();

        public WorkerHandler(Socket sock) throws IOException {
            this.sock = sock;
            this.in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            this.out = new PrintWriter(sock.getOutputStream(), true);
        }

        @Override
        public void run() {
            try {
                String line;
                while ((line=in.readLine())!=null) {
                    if (line.startsWith("SIGNAL:")) signals.offer(line.substring(7));
                }
            } catch(IOException e) {
                System.err.println("Worker disconnected: " + e.getMessage());
            }
        }

        public void sendCommand(String cmd, List<String> args) {
            out.println(cmd + (args.isEmpty()?"":" "+String.join(" ", args)));
        }

        public void waitFor(String doneSignal) throws InterruptedException {
            while (!signals.take().equals(doneSignal));
        }
    }
}