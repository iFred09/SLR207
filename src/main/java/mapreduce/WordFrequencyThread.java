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
        // System.out.println("Thread finished for file: " + inputFile);
    }

    public HashMap<String, Integer> getWordFrequency() {
        return wordFrequency;
    }
}