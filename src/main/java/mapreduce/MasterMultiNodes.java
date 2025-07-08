package mapreduce;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class MasterMultiNodes {
    private final int port = 5000;          // Port d'écoute fixe
    private final String textsPath;          // Chemin vers un dossier ou fichier .wet
    private final List<WorkerHandler> workers = new CopyOnWriteArrayList<>();
    private final ExecutorService exec = Executors.newCachedThreadPool();
    private ServerSocket server;            // Serveur principal

    public MasterMultiNodes(String textsPath) {
        this.textsPath = textsPath;
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
        exec.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Socket sock = server.accept();
                    WorkerHandler handler = new WorkerHandler(sock);
                    workers.add(handler);
                    exec.submit(handler);
                } catch (IOException e) {
                    System.err.println("Error accepting worker connection: " + e.getMessage());
                    if (server.isClosed()) break;
                }
            }
        });
    }

    /**
     * Exécute les phases Map, Shuffle, Reduce en séquence.
     * Gère le cas où textsPath est un dossier ou un fichier unique.
     */
    public void runPipeline() throws InterruptedException {
        // Lecture de la liste des Workers
        List<String> workersList;
        try {
            workersList = Files.readAllLines(Paths.get("workers.txt"));
        } catch (IOException e) {
            System.err.println("Cannot read workers.txt: " + e.getMessage());
            return;
        }
        int nWorkers = workersList.size();
        if (nWorkers == 0) {
            System.err.println("No workers defined in workers.txt");
            return;
        }

        // Préparation du répertoire de splits
        Path splitsDir = Paths.get("splits");
        try {
            if (Files.exists(splitsDir)) {
                Files.walk(splitsDir)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
            Files.createDirectories(splitsDir);
        } catch (IOException e) {
            System.err.println("Cannot prepare splits directory: " + e.getMessage());
            return;
        }

        // Comptage du nombre de lignes total dans le .wet
        long totalLines = 0;
        Path inputFile = Paths.get(textsPath);
        try (BufferedReader countReader = Files.newBufferedReader(inputFile)) {
            while (countReader.readLine() != null) totalLines++;
        } catch (IOException e) {
            System.err.println("Error counting lines in " + textsPath + ": " + e.getMessage());
            return;
        }

        // Calcul des tailles de splits
        long base = totalLines / nWorkers;
        long rem = totalLines % nWorkers;

        // Création des fichiers de splits
        List<String> splits = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            for (int i = 0; i < nWorkers; i++) {
                long linesToWrite = base + (i < rem ? 1 : 0);
                Path splitFile = splitsDir.resolve("split" + i + ".wet");
                try (BufferedWriter writer = Files.newBufferedWriter(splitFile)) {
                    for (long j = 0; j < linesToWrite; j++) {
                        String line = reader.readLine();
                        if (line == null) break;
                        writer.write(line);
                        writer.newLine();
                    }
                }
                splits.add(splitFile.toString());
            }
        } catch (IOException e) {
            System.err.println("Error creating splits: " + e.getMessage());
            return;
        }

        // Laisser le temps aux Workers de se connecter
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
        try { server.close(); } catch (IOException ignored) {}
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
        if (args.length < 2) {
            System.err.println("Usage: java MasterMultiNodes <wetFile> <masterIp>");
            return;
        }
        String textsPath = args[0];
        String masterIp = args[1];
        System.out.println("Master IP set to " + masterIp);
        MasterMultiNodes master = new MasterMultiNodes(textsPath);
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