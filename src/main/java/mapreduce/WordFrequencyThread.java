package mapreduce;

import java.util.HashMap;

public class WordFrequencyThread implements Runnable {
    private String inputFile;
    private HashMap<String, Integer> wordFrequency;

    public WordFrequencyThread(String inputFile) {
        this.inputFile = inputFile;
        this.wordFrequency = new HashMap<>();
    }

    @Override
    public void run() {
        // System.out.println("Thread started for file: " + inputFile);
        Words wordsObj = new Words();
        this.wordFrequency = wordsObj.frequencyWords(inputFile);
        StringBuilder sb = new StringBuilder();
        sb.append("Top 20 most frequent words for file: ")
                .append(inputFile)
                .append("\n");
        wordFrequency.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .limit(20)
                .forEach(e -> sb.append(e.getKey())
                        .append(": ")
                        .append(e.getValue())
                        .append("\n"));
        System.out.println(sb.toString());
        // System.out.println("Thread finished for file: " + inputFile);
    }

    public HashMap<String, Integer> getWordFrequency() {
        return wordFrequency;
    }
}