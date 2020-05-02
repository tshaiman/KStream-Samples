package join_global_ktable;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamApp {
    public static void main(String[] args) {
        KafkaStreams streams = buildTopology();
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-pipe-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }

    public static KafkaStreams buildTopology() {
        String topic = "bank-transactions";

        Properties StreamConfig = new Properties();
        StreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-ktable-join-app");
        StreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        StreamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        StreamConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        StreamConfig.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1");



        StreamsBuilder builder = new StreamsBuilder();


        return new KafkaStreams(builder.build(), StreamConfig);

    }

}
