package bank_transaction;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class Producer {

    public static String topic = "bank-transactions";
    public static void main(String[] args) throws InterruptedException {

        KafkaProducer<String, String> producer = GetProducer();
        int i=0;

        while(i < 100) {
            producer.send(GenRandomRecord("John"));
            Thread.sleep(100);
            producer.send(GenRandomRecord("Alice"));
            Thread.sleep(100);
            producer.send(GenRandomRecord("Bob"));
            Thread.sleep(100);
            i++;
        }
        System.out.println("done");

    }

    public static KafkaProducer<String,String> GetProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"20");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        return new KafkaProducer<>(properties);
    }

    public static ProducerRecord<String,String> GenRandomRecord(String name) {
        ObjectNode root = JsonNodeFactory.instance.objectNode();
        root.put("name",name);
        root.put("amount",ThreadLocalRandom.current().nextInt(100));
        root.put("time", Instant.now().getEpochSecond());
        System.out.println(root.toString());
        return new ProducerRecord<>(topic, name, root.toString());
    }
}
