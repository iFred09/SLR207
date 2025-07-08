package mapreduce;

import java.util.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ChunkWordFrequencyThread implements Runnable {
    private final List<String> lines;
    private final Map<String, Integer> wordFrequency = new HashMap<>();
    private final int id;
    private final int numWorkers;
    private ChunkWordFrequencyThread[] workersRef;
    private final Map<Integer, Map<String, Integer>> shuffleBuckets;
    private final Map<String, Integer> receivedData = new ConcurrentHashMap<>();

    public ChunkWordFrequencyThread(List<String> lines, int id, int numWorkers) {
        this.lines = lines;
        this.id = id;
        this.numWorkers = numWorkers;
        this.shuffleBuckets = new HashMap<>();
    }

    @Override
    public void run() {
        for (String line : lines) {
            String[] tokens = line
                    .toLowerCase()
                    .replaceAll("[^a-z0-9\\s]", "")
                    .split("\\s+");
            for (String word : tokens) {
                if (!word.isEmpty()) {
                    wordFrequency.merge(word, 1, Integer::sum);
                }
            }
        }
    }

    public Map<String, Integer> getWordFrequency() {
        return wordFrequency;
    }

    /**
     * Set references to all workers for shuffle communication.
     */
    public void setWorkersRef(ChunkWordFrequencyThread[] workersRef) {
        this.workersRef = workersRef;
    }

    /**
     * Prepare data buckets by computing hash modulo numWorkers.
     */
    public void prepareShuffle() {
        for (Map.Entry<String, Integer> entry : wordFrequency.entrySet()) {
            int dest = (entry.getKey().hashCode() & Integer.MAX_VALUE) % numWorkers;
            shuffleBuckets
                    .computeIfAbsent(dest, k -> new HashMap<>())
                    .put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Send each bucket to its destination worker.
     */
    public void executeShuffle() {
        for (Map.Entry<Integer, Map<String, Integer>> bucket : shuffleBuckets.entrySet()) {
            int dest = bucket.getKey();
            workersRef[dest].receiveShuffleData(bucket.getValue());
        }
    }

    /**
     * Receive incoming data for reduction.
     */
    public synchronized void receiveShuffleData(Map<String, Integer> data) {
        for (Map.Entry<String, Integer> entry : data.entrySet()) {
            receivedData.merge(entry.getKey(), entry.getValue(), Integer::sum);
        }
    }

    public Map<String, Integer> getReceivedData() {
        return receivedData;
    }
}