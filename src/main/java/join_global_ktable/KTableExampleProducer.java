package join_global_ktable;

import io.confluent.examples.streams.avro.Customer;
import io.confluent.examples.streams.avro.EnrichedOrder;
import io.confluent.examples.streams.avro.Order;
import io.confluent.examples.streams.avro.Product;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class KTableExampleProducer {
    private static final Random RANDOM = new Random();
    private static final int RECORDS_TO_GENERATE = 100;
    static final String ORDER_TOPIC = "order";
    static final String CUSTOMER_TOPIC = "customer";
    static final String PRODUCT_TOPIC =  "product";
    static final String CUSTOMER_STORE = "customer-store";
    static final String PRODUCT_STORE = "product-store";
    static final String ENRICHED_ORDER_TOPIC = "enriched-order";

    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        try {
            generateCustomers(bootstrapServers, schemaRegistryUrl);
            generateProducts(bootstrapServers, schemaRegistryUrl);
            generateOrders(bootstrapServers, schemaRegistryUrl, RECORDS_TO_GENERATE, RECORDS_TO_GENERATE);
            receiveEnrichedOrders(bootstrapServers, schemaRegistryUrl, RECORDS_TO_GENERATE);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static void generateOrders(String bootstrapServers, String schemaRegistryUrl, int numCustomers, int numProducts) {
        final SpecificAvroSerde<Order> orderSerde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        orderSerde.configure(serdeConfig, false);

        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        final KafkaProducer<Long,Order> producer = new KafkaProducer<>(producerProperties, Serdes.Long().serializer(), orderSerde.serializer());
        for(long i = 0; i < KTableExampleProducer.RECORDS_TO_GENERATE; ++i ) {
            Order order = new Order((long)RANDOM.nextInt(numCustomers),(long)RANDOM.nextInt(numProducts),RANDOM.nextLong());
            producer.send(new ProducerRecord<>(ORDER_TOPIC, i, order));
        }
        producer.close();

    }

    private static void generateCustomers(String bootstrapServers, String schemaRegistryUrl) throws ExecutionException, InterruptedException {
        final SpecificAvroSerde<Customer> customerSerde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        customerSerde.configure(serdeConfig, false);

        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        final KafkaProducer<Long,Customer> producer = new KafkaProducer<>(producerProperties, Serdes.Long().serializer(), customerSerde.serializer());
        final String [] genders = {"male", "female", "unknown"};
        final Random random = new Random();
        for(long i = 0; i < KTableExampleProducer.RECORDS_TO_GENERATE; ++i) {
            Customer c = new Customer(randomString(10),genders[random.nextInt(genders.length)],randomString(10));
            producer.send(new ProducerRecord<>(CUSTOMER_TOPIC,i,c)).get();
        }
        producer.close();
    }

    private static void generateProducts(String bootstrapServers, String schemaRegistryUrl) throws ExecutionException, InterruptedException {
        final SpecificAvroSerde<Product> productSerde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        productSerde.configure(serdeConfig, false);

        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        final KafkaProducer<Long,Product> producer = new KafkaProducer<>(producerProperties,Serdes.Long().serializer(),productSerde.serializer());


        for(long i = 0; i < KTableExampleProducer.RECORDS_TO_GENERATE; ++i) {
            Product p = new Product(randomString(10),randomString(100),randomString(20));
            producer.send(new ProducerRecord<>(PRODUCT_TOPIC,i,p)).get();
        }
        producer.close();
    }


    private static void receiveEnrichedOrders(
            final String bootstrapServers,
            final String schemaRegistryUrl,
            final int expected
    ) {
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "global-tables-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        final KafkaConsumer<Long, EnrichedOrder> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(ENRICHED_ORDER_TOPIC));
        int received = 0; 
        while(received < expected) {
            final ConsumerRecords<Long, EnrichedOrder> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            records.forEach(record -> System.out.println(record.value()));
            received += records.count();
        }
        consumer.close();
    }
    private static String randomString(final int len) {
        final StringBuilder b = new StringBuilder();
        final String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        for(int i = 0; i < len; ++i) {
            b.append(base.charAt(RANDOM.nextInt(base.length())));
        }

        return b.toString();
    }

}
