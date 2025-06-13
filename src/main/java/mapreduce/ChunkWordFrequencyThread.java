package mapreduce;

import java.util.HashMap;
import java.util.List;

public class ChunkWordFrequencyThread implements Runnable {
    private List<String> lines;
    private HashMap<String, Integer> wordFrequency;

    public ChunkWordFrequencyThread(List<String> lines) {
        this.lines = lines;
        this.wordFrequency = new HashMap<>();
    }

    @Override
    public void run() {
        Words words = new Words();
        for (String line : lines) {
            String[] tokens = line.toLowerCase().replaceAll("[^a-z0-9\\s]", "").split("\\s+");
            for (String word : tokens) {
                if (!word.isEmpty()) {
                    wordFrequency.put(word, wordFrequency.getOrDefault(word, 0) + 1);
                }
            }
        }
    }

    public HashMap<String, Integer> getWordFrequency() {
        return wordFrequency;
    }
}