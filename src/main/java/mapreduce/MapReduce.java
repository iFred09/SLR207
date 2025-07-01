package mapreduce;

import java.io.*;
import java.util.*;

public class MapReduce {
    public static void main(String[] args) {
        String inputFile = args[0];
        int numberOfThreads = Integer.parseInt(args[1]);

        List<String> allLines = new ArrayList<>();

        // 🔹 Load all lines into memory
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                allLines.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // 🔹 Split lines into chunks
        int totalLines = allLines.size();
        int chunkSize = totalLines / numberOfThreads;
        ChunkWordFrequencyThread[] workers = new ChunkWordFrequencyThread[numberOfThreads];
        Thread[] threads = new Thread[numberOfThreads];

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numberOfThreads; i++) {
            int start = i * chunkSize;
            int end = (i == numberOfThreads - 1) ? totalLines : (i + 1) * chunkSize;
            List<String> chunk = allLines.subList(start, end);

            workers[i] = new ChunkWordFrequencyThread(chunk);
            threads[i] = new Thread(workers[i]);
            threads[i].start();
        }

        // 🔹 Join threads
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // 🔹 Reduce: Merge all thread results
        HashMap<String, Integer> finalFreq = new HashMap<>();
        for (ChunkWordFrequencyThread worker : workers) {
            for (Map.Entry<String, Integer> entry : worker.getWordFrequency().entrySet()) {
                finalFreq.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
        }

        // 🔹 Output: Top 20 most frequent words
        StringBuilder sb = new StringBuilder();
        sb.append("Top 20 most frequent words for file: ")
                .append(inputFile)
                .append("\n");
        finalFreq.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .limit(20)
                .forEach(e -> sb.append(e.getKey())
                        .append(": ")
                        .append(e.getValue())
                        .append("\n"));
        System.out.println(sb.toString());
        long endTime = System.currentTimeMillis();
        System.out.println("MapReduce on single file completed in " + (endTime - startTime) + " ms");
    }
}