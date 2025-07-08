package mapreduce;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class MasterMultiNodes {
    private final int port = 5000;          // Port d'écoute fixe
    private final String textsDir;          // Répertoire contenant les fichiers .wet
    private final List<WorkerHandler> workers = new CopyOnWriteArrayList<>();
    private final ExecutorService exec = Executors.newCachedThreadPool();
    private ServerSocket server;            // Serveur principal

    public MasterMultiNodes(String textsDir) {
        this.textsDir = textsDir;
    }

    /**
     * Démarre le serveur Master et accepte les connexions des Workers.
     */
    public void start() {
        try {
            server = new ServerSocket(port);
            System.out.println("Master listening on port " + port);
        } catch (IOException e) {
            System.err.println("Failed to start Master on port " + port + ": " + e.getMessage());
            return;
        }

        // Accept worker connections asynchronously
        exec.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Socket sock = server.accept();
                    WorkerHandler handler = new WorkerHandler(sock);
                    workers.add(handler);
                    exec.submit(handler);
                } catch (IOException e) {
                    System.err.println("Error accepting worker connection: " + e.getMessage());
                    // If server is closed, break
                    if (server.isClosed()) break;
                }
            }
        });
    }

    /**
     * Exécute les phases Map, Shuffle, Reduce en séquence.
     */
    public void runPipeline() throws InterruptedException {
        // Chargement des splits depuis le répertoire textsDir
        List<String> splits = new ArrayList<>();
        try (Stream<Path> paths = Files.list(Paths.get(textsDir))) {
            splits = paths
                    .filter(p -> p.toString().endsWith(".wet"))
                    .map(Path::toString)    // chemin complet
                    .toList();
        } catch (IOException e) {
            System.err.println("Error reading splits in " + textsDir + ": " + e.getMessage());
            return;
        }

        // Donne le temps aux Workers de se connecter
        Thread.sleep(2000);

        // MAP phase
        long t0 = System.currentTimeMillis();
        broadcast("MAP", splits);
        waitForPhase("MAP_DONE");
        System.out.println("MAP FINISHED in " + (System.currentTimeMillis() - t0) + " ms");

        // SHUFFLE phase
        long t1 = System.currentTimeMillis();
        broadcast("SHUFFLE", Collections.emptyList());
        waitForPhase("SHUFFLE_DONE");
        System.out.println("SHUFFLE FINISHED in " + (System.currentTimeMillis() - t1) + " ms");

        // REDUCE phase
        long t2 = System.currentTimeMillis();
        broadcast("REDUCE", Collections.emptyList());
        waitForPhase("REDUCE_DONE");
        System.out.println("REDUCE FINISHED in " + (System.currentTimeMillis() - t2) + " ms");

        // Arrêt du serveur
        try {
            server.close();
        } catch (IOException ignored) {}
        exec.shutdown();
    }

    private void broadcast(String cmd, List<String> args) {
        for (WorkerHandler w : workers) {
            w.sendCommand(cmd, args);
        }
    }

    private void waitForPhase(String doneSignal) throws InterruptedException {
        for (WorkerHandler w : workers) {
            w.waitFor(doneSignal);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 1) {
            System.err.println("Usage: java MasterMultiNodes <textsDir>");
            return;
        }
        MasterMultiNodes master = new MasterMultiNodes(args[0]);
        master.start();
        master.runPipeline();
    }

    /**
     * Classe interne pour gérer un Worker connecté :
     * - Lit les signaux SIGNAL:...
     * - Envoie les commandes MAP/SHUFFLE/REDUCE
     */
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
                while ((line = in.readLine()) != null) {
                    if (line.startsWith("SIGNAL:")) {
                        signals.offer(line.substring(7));
                    }
                }
            } catch (IOException e) {
                System.err.println("Worker disconnected: " + e.getMessage());
            }
        }

        public void sendCommand(String cmd, List<String> args) {
            out.println(cmd + (args.isEmpty() ? "" : " " + String.join(" ", args)));
        }

        public void waitFor(String doneSignal) throws InterruptedException {
            String sig;
            do {
                sig = signals.take();
            } while (!sig.equals(doneSignal));
        }
    }
}