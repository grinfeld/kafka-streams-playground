package com.mikerusoft.cdc.serdes.global;

import com.mikerusoft.cdc.serdes.DebeziumJsonSerde;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mikerusoft.cdc.serdes.model.Customer;
import com.mikerusoft.cdc.serdes.model.MutablePair;
import com.mikerusoft.cdc.serdes.model.SectionData;
import com.mikerusoft.cdc.serdes.model.SectionKey;
import com.mikerusoft.cdc.serdes.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

@Slf4j
@Disabled
public class GlobalTests {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Consumed<Integer, Customer> DEBEZIUM_CONSUMED = Consumed.with(
            new DebeziumJsonSerde<>(mapper, Integer.class, true),
            new DebeziumJsonSerde<>(mapper, Customer.class, false)
    );
    private static final JsonSerde<Customer> CUSTOMER_JSON_SERDE = new JsonSerde<>(mapper, Customer.class);

    @Test
    void createTopicForGlobalState() throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.CONSUMER_PREFIX, "" + System.currentTimeMillis());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId" + System.currentTimeMillis());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Integer, Customer> stream = builder.stream("stamname.inventory.customers", DEBEZIUM_CONSUMED);
        stream.to("customers", Produced.with(Serdes.Integer(), CUSTOMER_JSON_SERDE));
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
            } catch (Exception e) {
                log.error("Failed to close stream", e);
            }
        }));
        log.info(topology.describe().toString());
        streams.start();
        Thread.sleep(TimeUnit.MINUTES.toMillis(10));
    }

    @Test
    // globalStore and stream couldn't read from the same topic :(
    // in case of CDC globalStore shouldn't be the CDC topic, but additional topic, because of Serializer/Deserializer API for stores we have in Kafka Stream
    void readFromGlobalStore() throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.CONSUMER_PREFIX, "" + System.currentTimeMillis());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId" + System.currentTimeMillis());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");

        StreamsBuilder builder = new StreamsBuilder();

        KeyValueBytesStoreSupplier sectionsStoreSupplier =
                Stores.inMemoryKeyValueStore("queryable-customers-store");
        Materialized<Integer, Customer, KeyValueStore<Bytes, byte[]>> as =
                Materialized.as(sectionsStoreSupplier);

        String storeName = builder
            .globalTable("customers", Consumed.with(Serdes.Integer(), CUSTOMER_JSON_SERDE), as).queryableStoreName();

        KStream<String, String> stream = builder.stream("stamname.inventory.orders", Consumed.with(Serdes.String(), Serdes.String()));
        KStreamWrapper.of(stream).transformValuesWithGlobalStore(new BiFunction<String, KeyValueStore<Integer,Customer>, String>() {
            @Override
            public String apply(String s, KeyValueStore<Integer, Customer> store) {
                Customer customer = store.get(1004);
                return s;
            }
        }, storeName).getStream()
        .foreach((key, value) -> log.info(key + "->" + value));


        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
            } catch (Exception e) {
                log.error("Failed to close stream", e);
            }
        }));
        log.info(topology.describe().toString());
        streams.start();

        /*
        // in case we have non kafka-stream application, and we want to use globalKTable to query it as simple cache
        ReadOnlyKeyValueStore<Integer, Customer> view =
                waitUntilStoreIsQueryable(StoreQueryParameters.fromNameAndType("queryable-customers-store", QueryableStoreTypes.keyValueStore()), streams);
        */

        Thread.sleep(TimeUnit.MINUTES.toMillis(10));
    }

    public static <T> T waitUntilStoreIsQueryable(StoreQueryParameters<T> storeQueryParameters,
                                                  final KafkaStreams streams) throws InterruptedException {
        while (true) {
            try {
                return streams.store(storeQueryParameters);
            } catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                Thread.sleep(100);
            }
        }
    }

    @Test
    void customHashFunction() {
        Properties props = new Properties();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        props.put("partitioner.class", KeyPartitioner.class.getName());

        KafkaProducer<SectionKey, SectionData> producer = new KafkaProducer<>(props, new KeySerializer(), new JsonSerializer<>(mapper));

        final Random r = new Random();

        IntStream.range(100, 800).mapToObj(i ->
            new SectionData(i % 10, i % 10L, r.nextLong() % 1000, System.currentTimeMillis())
        ).forEach(s -> sendToSectionsTopic(producer, s));
    }

    @Test
    void groupByKeyAndggregateWhenPartitioningIsDifferentFromActualKey() throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.CONSUMER_PREFIX, "" + System.currentTimeMillis());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId" + System.currentTimeMillis());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, SectionData> stream = builder.stream("sections", Consumed.with(Serdes.String(), new JsonSerde<>(mapper, SectionData.class)));
        stream.groupByKey()
            .aggregate(
                () -> new MutablePair<>(null, 0L),
                (key, value, aggregate) ->
                {
                    aggregate.setKey(value);
                    aggregate.updateValueWith(v -> v + 1);
                    return aggregate;
                },
                Materialized.with(Serdes.String(), new JsonSerde<>(mapper, new TypeReference<MutablePair<SectionData, Long>>(){}))
            )
            .filter((key, pair) -> pair.getKey() != null)
            .filter((key, pair) -> {
                if (pair.getValue() > 1)
                    log.info(" ------ " + pair);
                return pair.getValue() == 1;
            })
            .toStream()
            .mapValues(MutablePair::getKey)
            .foreach((key, t) -> System.out.println(t));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
            } catch (Exception e) {
                log.error("Failed to close stream", e);
            }
        }));
        log.info(topology.describe().toString());
        streams.start();
        Thread.sleep(TimeUnit.MINUTES.toMillis(10));
    }

    private static void sendToSectionsTopic(KafkaProducer<SectionKey, SectionData> producer, SectionData s) {
        try {
            producer.send(new ProducerRecord<>("sections", SectionKey.buildFrom(s), s)).get();
        } catch (Exception e) {
            Utils.rethrow(e);
        }
    }

}
