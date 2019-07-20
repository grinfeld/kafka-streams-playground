package com.mikerusoft.playground.kafkastreamsinit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

public class KafkaStreamUtils {

    public static <V> KStream<String, V> createStringJsonStream(Class<V> valueClass, String topicFrom, StreamsBuilder sb) {
        return sb.stream(topicFrom,
            Consumed.with(
                Serdes.String(),
                new JSONSerde<>(valueClass)
            )
        );
    }

    public static <V> Properties streamProperties(String appId, String url, Class<V> valueClass) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        config.put(StreamsConfig.CLIENT_ID_CONFIG, appId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, url);

        // setting default serialization, if we need it
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class.getName());
        config.put("JsonPOJOClass", valueClass);
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);


        // setting offset reset to earliest so that we can re-run the example with the same pre-loaded data
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return config;
    }

    public static void runStream(final KafkaStreams stream) {
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

        try {
            stream.cleanUp(); // not for production :)
            stream.start();
        } catch (final Throwable e) {
            e.printStackTrace();
        }
    }

    public static void runStream(final KafkaStreams...streams) {
        Runtime.getRuntime()
            .addShutdownHook(new Thread(() -> Stream.of(streams).forEach(s -> { try { s.close(); } catch (Exception ignore){}})));

        try {
            Stream.of(streams).forEach(KafkaStreams::cleanUp); // not for production :)
            Stream.of(streams).forEach(s -> { try { s.start(); } catch (Exception ignore){}});
        } catch (final Throwable e) {
            e.printStackTrace();
        }
    }


    public static <V> Produced<String, V> createProduced(Class<V> clazz) {
        return Produced.with(Serdes.String(), new JSONSerde<>(clazz));
    }

    public static <V> Materialized<String, V, KeyValueStore<Bytes, byte[]>> materialized(Class<V> clazz) {
        return Materialized.with(Serdes.String(), new JSONSerde<>(clazz));
    }

    public static <V> Materialized<String, V, WindowStore<Bytes, byte[]>> materializedWindow(Class<V> clazz) {
        return Materialized.with(Serdes.String(), new JSONSerde<>(clazz));
    }
}
