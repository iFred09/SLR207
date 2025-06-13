package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Words {
    public int countWords(String inputFile) {
        if (inputFile == null || inputFile.isEmpty()) {
            return 0;
        }
        
        int wordCount = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                wordCount += line.split("\\s+").length; // Compte les mots dans chaque ligne
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return wordCount;
    }
    public HashMap<String, Integer> frequencyWords(String inputFile){
        HashMap<String, Integer> wordFrequency = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] words = line.split("\\s+");
                for (String word : words) {
                    wordFrequency.put(word, wordFrequency.getOrDefault(word, 0) + 1);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return wordFrequency;
    }
}