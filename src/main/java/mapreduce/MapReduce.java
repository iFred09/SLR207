package mapreduce;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class MapReduce {
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.err.println("Usage: java MapReduce <input-file> <num-threads>");
            System.exit(1);
        }

        // ----------- READ FILE AND ARGS -----------
        List<String> allLines = Files.readAllLines(Paths.get(args[0]));
        int numberOfThreads = Integer.parseInt(args[1]);

        long startTime = System.currentTimeMillis();

        // ------------ MAP PHASE ------------
        ChunkWordFrequencyThread[] workers = new ChunkWordFrequencyThread[numberOfThreads];
        Thread[] threads = new Thread[numberOfThreads];
        int totalLines = allLines.size();
        int chunkSize  = totalLines / numberOfThreads;

        for (int i = 0; i < numberOfThreads; i++) {
            int start = i * chunkSize;
            int end = (i == numberOfThreads - 1) ? totalLines : (i + 1) * chunkSize;
            List<String> chunk = allLines.subList(start, end);
            workers[i] = new ChunkWordFrequencyThread(chunk, i, numberOfThreads);
            threads[i] = new Thread(workers[i]);
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join();
        }
        System.out.println("MAP FINISHED");

        // ------------ SHUFFLE PHASE ------------
        // 1. Master sends list of workers to each worker
        for (ChunkWordFrequencyThread w : workers) {
            w.setWorkersRef(workers);
        }
        // 2. Launch shuffle threads
        Thread[] shuffleThreads = new Thread[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            final int idx = i;
            shuffleThreads[i] = new Thread(() -> {
                workers[idx].prepareShuffle();
                workers[idx].executeShuffle();
            });
            shuffleThreads[i].start();
        }
        // 3. Wait for shuffle completion
        for (Thread t : shuffleThreads) {
            t.join();
        }
        System.out.println("SHUFFLE FINISHED");

        // ------------ REDUCE PHASE ------------
        Map<String, Integer> finalFreq = new HashMap<>();
        for (ChunkWordFrequencyThread w : workers) {
            for (Map.Entry<String, Integer> entry : w.getReceivedData().entrySet()) {
                finalFreq.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
        }
        System.out.println("REDUCE FINISHED");
        // Print results
        StringBuilder sb = new StringBuilder();
        sb.append("Top 20 words: \n");
        finalFreq.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .limit(20)
                .forEach(e -> sb.append(e.getKey())
                        .append(": ")
                        .append(e.getValue())
                        .append("\n"));
        System.out.print(sb.toString());
        long endTime = System.currentTimeMillis();
        System.out.println("MapReduce on file completed in " + (endTime - startTime) + " ms");
    }
}