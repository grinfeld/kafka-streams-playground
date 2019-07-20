package com.mikerusoft.playground.simple.streams;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.models.events.Event;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
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

        Properties timeStoreConfig = KafkaStreamUtils.streamProperties("events-stream-for-time" + UUID.randomUUID().toString(), url, Event.class);
        final StreamsBuilder timeStoreBuilder = new StreamsBuilder();
        KeyValueBytesStoreSupplier supplier = Stores.persistentKeyValueStore("events-stream-time-store");
        KTable<String, Long> ktable = timeStoreBuilder
                .stream("events-stream", Consumed.with(Serdes.String(), new JSONSerde<>(Event.class)))
                .mapValues(Event::getTimestamp)
                .groupByKey().aggregate(() -> 0L, (key, value, aggregate) -> value < aggregate ? aggregate : value,
                    defineAggregateStore(supplier)
                );
        String timeStoreName = ktable.queryableStoreName();

        Topology timeStoreTopology = timeStoreBuilder.build();
        System.out.println("" + timeStoreTopology.describe());
        KafkaStreamUtils.runStream(
            new KafkaStreams(timeStoreTopology, timeStoreConfig)
        );

        Properties mainStreamConfig = KafkaStreamUtils.streamProperties("events-stream-main" + UUID.randomUUID().toString(), url, Event.class);
        final StreamsBuilder mainStreamBuilder = new StreamsBuilder();
        mainStreamBuilder.stream("events-stream", Consumed.with(Serdes.String(), new JSONSerde<>(Event.class)))
            .peek((k, ev) -> System.out.println(k + " -> " + ev))
            .transformValues(new ValueTransformerWithKeySupplier<String, Event, Event>() {
                @Override
                public ValueTransformerWithKey<String, Event, Event> get() {
                    return new ValueTransformerWithKey<String, Event, Event>() {
                        private KeyValueStore<String, Long> state;

                        @Override
                        public void init(ProcessorContext context) {
                            state = (KeyValueStore<String, Long>)context.getStateStore(timeStoreName);
                        }

                        @Override
                        public Event transform(String key, Event value) {
                            if (state == null)
                                return value;
                            Long timeInStore = state.get(key);
                            if (timeInStore == null)
                                return value;

                            if (value.getTimestamp() > timeInStore)
                                return value;

                            return value.toBuilder().timestamp(timeInStore).build();
                        }

                        @Override
                        public void close() {}
                    };
                }
            }, new String[]{timeStoreName})
            .leftJoin(ktable, (value1, value2) -> value2 == null || value1.getTimestamp() <= value2 ?
                    value1 : value1.toBuilder().timestamp(value2).build())
            .filter((k, e) -> e != null)
            .to("events-after-join", Produced.with(Serdes.String(), new JSONSerde<>(Event.class)));

        Topology mainStreamTopology = mainStreamBuilder.build();
        System.out.println("" + mainStreamTopology.describe());

        KafkaStreamUtils.runStream(
            new KafkaStreams(mainStreamTopology, mainStreamConfig)
        );
    }
}
