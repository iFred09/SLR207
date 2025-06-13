package mapreduce;

public class WordCountThread implements Runnable {
    private String inputFile;
    private int wordCount;

    public WordCountThread(String inputFile) {
        this.inputFile = inputFile;
        this.wordCount = 0;
    }

    @Override
    public void run() {
        // System.out.println("Thread started for file: " + inputFile);
        Words wordsObj = new Words();
        this.wordCount = wordsObj.countWords(inputFile);
        System.out.println("Word count for file " + inputFile + ": " + wordCount);
        // System.out.println("Thread finished for file: " + inputFile);
    }

    public int getWordCount() {
        return wordCount;
    }
}
