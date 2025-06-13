package mapreduce;

import java.lang.Thread;

public class TestWordCountAndFrequency {
    public static void main(String[] args) {
        WordCountThread t1 = new WordCountThread("texts/CC-MAIN-20230321002050-20230321032050-00472.warc.wet");
        WordCountThread t2 = new WordCountThread("texts/CC-MAIN-20230321002050-20230321032050-00446.warc.wet");
        WordCountThread t3 = new WordCountThread("texts/CC-MAIN-20230321002050-20230321032050-00460.warc.wet");
        WordCountThread t4 = new WordCountThread("texts/CC-MAIN-20230321002050-20230321032050-00486.warc.wet");

        Thread thread1 = new Thread(t1);
        Thread thread2 = new Thread(t2);
        Thread thread3 = new Thread(t3);
        Thread thread4 = new Thread(t4);

        long startTime = System.currentTimeMillis();
        thread1.start();
        thread2.start();
        thread3.start();
        thread4.start();
        try {
            thread1.join();
            thread2.join();
            thread3.join();
            thread4.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Total time taken to start threads: " + (endTime - startTime) + " ms");

        WordFrequencyThread t5 = new WordFrequencyThread("texts/CC-MAIN-20230321002050-20230321032050-00472.warc.wet");
        WordFrequencyThread t6 = new WordFrequencyThread("texts/CC-MAIN-20230321002050-20230321032050-00446.warc.wet");
        WordFrequencyThread t7 = new WordFrequencyThread("texts/CC-MAIN-20230321002050-20230321032050-00460.warc.wet");
        WordFrequencyThread t8 = new WordFrequencyThread("texts/CC-MAIN-20230321002050-20230321032050-00486.warc.wet");
        Thread thread5 = new Thread(t5);
        Thread thread6 = new Thread(t6);
        Thread thread7 = new Thread(t7);
        Thread thread8 = new Thread(t8);

        startTime = System.currentTimeMillis();
        thread5.start();
        thread6.start();
        thread7.start();
        thread8.start();
        try {
            thread5.join();
            thread6.join();
            thread7.join();
            thread8.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        endTime = System.currentTimeMillis();
        System.out.println("Total time taken to start threads: " + (endTime - startTime) + " ms");
    }
}