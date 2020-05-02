package bank_transaction;

import bank_transaction.BankDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import serializers.JsonPOJODeserializer;
import serializers.JsonPOJOSerializer;


import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
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
        StreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application6");
        StreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        StreamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        StreamConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamConfig.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1");
        // Exactly once processing!!
        //StreamConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);


        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<Transaction> transactionSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", Transaction.class);
        transactionSerializer.configure(serdeProps, false);

        final Deserializer<Transaction> transactionDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", Transaction.class);
        transactionDeserializer.configure(serdeProps, false);


        final Serializer<BankBalance> balanceSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", BankBalance.class);
        balanceSerializer.configure(serdeProps, false);

        final Deserializer<BankBalance> balanceDeserializer = new BankDeserializer();


        final Serde<BankBalance> bankSerde = Serdes.serdeFrom(balanceSerializer, balanceDeserializer);
        final Serde<Transaction> transactionSerde = Serdes.serdeFrom(transactionSerializer, transactionDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Transaction> transStream = builder.stream(topic, Consumed.with(Serdes.String(), transactionSerde));
        BankBalance initBalance = new BankBalance();


        KTable<String, BankBalance> aggregasteStream = transStream.groupByKey().aggregate(
                () -> initBalance, /* initializer */
                (aggKey, transaction, balance) -> newBalance(transaction, balance),
                Materialized.<String, BankBalance, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store").withKeySerde(Serdes.String()).withValueSerde(bankSerde));

        aggregasteStream.toStream().to("exactly-once-topic", Produced.with(Serdes.String(), bankSerde));


        return new KafkaStreams(builder.build(), StreamConfig);

    }

    private static BankBalance newBalance(Transaction transaction, BankBalance balance) {
        balance.count++;
        balance.total += transaction.amount;
        balance.time = Math.max(balance.time, transaction.time);
        return balance;
    }

    public static class Transaction {
        public String name;
        public int amount;
        public long time;
    }

    public static class BankBalance {
        public String name;
        public int count;
        public int total;
        public long time;

        public BankBalance() {
            count = 0;
            total = 0;
            time = Instant.ofEpochMilli(0L).getEpochSecond();
        }
    }
}
