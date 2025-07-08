package MapReduceKafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class WorkerKafka {
    private static final String MAP_TOPIC = "map-topic";
    private static final String REDUCE_TOPIC = "reduce-topic";

    public static void main(String[] args) {
        // Configure consumer for map stage
        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "worker-group");
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps);
        consumer.subscribe(Collections.singletonList(MAP_TOPIC));

        // Configure producer for reduce stage
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        KafkaProducer<String, Integer> producer = new KafkaProducer<>(prodProps);

        // Consume lines and emit word counts
        boolean running = true;
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty()) {
                // no new data, continue polling
                continue;
            }
            records.forEach(record -> {
                String line = record.value();
                String[] tokens = line.toLowerCase().replaceAll("[^a-z0-9 ]", "").split("\\s+");
                for (String word : tokens) {
                    if (!word.isEmpty()) {
                        producer.send(new ProducerRecord<>(REDUCE_TOPIC, word, 1));
                    }
                }
            });
        }

        // cleanup
        // producer.close(); consumer.close(); // unreachable in this example
    }
}
