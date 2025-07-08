package MapReduceKafka;

/*
 * MasterKafka.java
 * Reads a text file, sends each line to the "map-topic", and then
 * consumes intermediate counts from "reduce-topic" to aggregate final word frequencies.
 */
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MasterKafka {
    private static final String MAP_TOPIC = "map-topic";
    private static final String REDUCE_TOPIC = "reduce-topic";

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: MasterKafka <input-file>");
            System.exit(1);
        }
        String inputFile = args[0];

        // Configure producer for map stage
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps);

        // Read file and send each line as a message
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                producer.send(new ProducerRecord<>(MAP_TOPIC, null, line));
            }
        }
        producer.flush();
        producer.close();
        System.out.println("All lines sent to map-topic.");

        // Configure consumer for reduce stage
        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "master-reducer");
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consProps);
        consumer.subscribe(Collections.singletonList(REDUCE_TOPIC));

        // Aggregate counts
        Map<String, Integer> finalCounts = new HashMap<>();
        boolean polling = true;
        while (polling) {
            ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) {
                // assume no more messages
                polling = false;
            } else {
                records.forEach(r -> {
                    finalCounts.merge(r.key(), r.value(), Integer::sum);
                });
            }
        }
        consumer.close();

        // Print results
        System.out.println("Word counts:");
        finalCounts.forEach((word, count) -> System.out.println(word + ": " + count));
    }
}