package com.mikerusoft.playground.simple.streams;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.kafkastreamsinit.SingleFieldSerdeForSerializer;
import com.mikerusoft.playground.models.events.Event;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.UUID;

// sh kafka-console-consumer --bootstrap-server localhost:9092 --topic events-stream --from-beginning --group tempme
// sh kafka-topics --zookeeper localhost:22181 --topic events-stream --describe

@Component("events-thorough")
public class SimpleEventStream implements Streamable {

    private static Materialized<String, Long, KeyValueStore<Bytes, byte[]>> defineAggregateStore(KeyValueBytesStoreSupplier supplier) {
        return Materialized.<String, Long>as(supplier)
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()).withCachingDisabled();
    }

    @Override
    public void runStream(String url) {
        Properties config1 = KafkaStreamUtils.streamProperties("events-stream-group1" + UUID.randomUUID().toString(), url, Event.class);
        Properties config2 = KafkaStreamUtils.streamProperties("events-stream-group2" + UUID.randomUUID().toString(), url, Event.class);
        final StreamsBuilder builder1 = new StreamsBuilder();
        builder1.stream("events-stream", Consumed.with(Serdes.String(), new JSONSerde<>(Event.class)))
            .peek( (k,ev) -> System.out.println(k + " -> " + ev))
            .through("events-stream-time", Produced.with(Serdes.String(),
                new SingleFieldSerdeForSerializer<>(Serdes.Long().serializer(), Event::getTimestamp))
            ).to("events-result-stream", Produced.with(Serdes.String(), new JSONSerde<>(Event.class)));

        Topology topology1 = builder1.build();
        System.out.println("" + topology1.describe());

        final StreamsBuilder builder2 = new StreamsBuilder();
        KeyValueBytesStoreSupplier supplier = Stores.persistentKeyValueStore("events-stream-time-store");
        KTable<String, Long> ktable = builder2.stream("events-stream-time", Consumed.with(Serdes.String(), Serdes.Long()))
                .groupByKey().aggregate(() -> 0L, (key, value, aggregate) -> value < aggregate ? aggregate : value, defineAggregateStore(supplier));


        builder2.stream("events-result-stream", Consumed.with(Serdes.String(), new JSONSerde<>(Event.class)))
            .peek((k,ev) -> System.out.println(ev))
            .leftJoin(ktable, (value1, value2) -> value2 == null || value1.getTimestamp() <= value2 ?
                    value1 : value1.toBuilder().timestamp(value2).build())
            .filter((k, e) -> e != null)
            .to("events-after-join", Produced.with(Serdes.String(), new JSONSerde<>(Event.class)));

        Topology topology2 = builder1.build();
        System.out.println("" + topology2.describe());

        KafkaStreamUtils.runStream(new KafkaStreams(topology1, config1), new KafkaStreams(topology2, config2));


    }
}
