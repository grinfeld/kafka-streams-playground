package com.mikerusoft.playground.simple.streams;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.models.events.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;

import static org.apache.kafka.streams.KafkaStreams.State.RUNNING;

// sh kafka-console-consumer --bootstrap-server localhost:9092 --topic events-stream --from-beginning --group tempme
// sh kafka-topics --zookeeper localhost:22181 --topic events-stream --describe

@Slf4j
@Component("events-thorough")
public class SimpleEventStream implements Streamable {

    private static Materialized<String, Long, KeyValueStore<Bytes, byte[]>> defineAggregateStore(String supplier) {
        return Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(supplier)
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()).withCachingDisabled();
    }

    @Override
    public void runStream(String url) {
        Properties timeStoreConfig = KafkaStreamUtils.streamProperties("events-stream-for-time" + UUID.randomUUID().toString(), url, Event.class);
        final StreamsBuilder timeStoreBuilder = new StreamsBuilder();
        KTable<String, Long> ktable = timeStoreBuilder
                .stream("events-stream", Consumed.with(Serdes.String(), new JSONSerde<>(Event.class)))
                .mapValues(Event::getTimestamp)
                .peek((k,v) -> System.out.println(k + " -- " + v ))
                .groupByKey()
                .aggregate(() -> 0L, (key, value, aggregate) -> value < aggregate ? aggregate : value,
                    defineAggregateStore("events-stream-time-store")
                );
        String timeStoreName = ktable.queryableStoreName();

        Topology timeStoreTopology = timeStoreBuilder.build();
        System.out.println("" + timeStoreTopology.describe());
        KafkaStreams timeStream = new KafkaStreams(timeStoreTopology, timeStoreConfig);
        timeStream.start();

        ScheduledExecutorService waitForQueryable = Executors.newScheduledThreadPool(1);

        waitForQueryStoreToBeReady(timeStoreName, timeStream, waitForQueryable);

        ReadOnlyKeyValueStore<String, Long> eventTimeStore =
                timeStream.store(timeStoreName, QueryableStoreTypes.keyValueStore());

        Properties mainStreamConfig = KafkaStreamUtils.streamProperties("events-stream-main" + UUID.randomUUID().toString(), url, Event.class);
        final StreamsBuilder mainStreamBuilder = new StreamsBuilder();
        mainStreamBuilder.stream("events-stream", Consumed.with(Serdes.String(), new JSONSerde<>(Event.class)))
            .peek((k, ev) -> System.out.println(k + " -> " + ev))
            .mapValues(new ValueMapperWithKey<String, Event, Event>() {
                @Override
                public Event apply(String key, Event value) {
                    if (eventTimeStore == null)
                        return value;
                    Long timeInStore = eventTimeStore.get(key);
                    if (timeInStore == null)
                        return value;

                    if (value.getTimestamp() > timeInStore)
                        return value;

                    return value.toBuilder().timestamp(timeInStore).build();
                }
            })
            .to("events-after-join", Produced.with(Serdes.String(), new JSONSerde<>(Event.class)));

        Topology mainStreamTopology = mainStreamBuilder.build();
        System.out.println("" + mainStreamTopology.describe());

        KafkaStreamUtils.runStream(
            new KafkaStreams(mainStreamTopology, mainStreamConfig)
        );
    }

    private void waitForQueryStoreToBeReady(String timeStoreName, KafkaStreams timeStream, ScheduledExecutorService waitForQueryable) {
        try {
            waitForQueryable.scheduleAtFixedRate(() -> {
                try {
                    timeStream.store(timeStoreName, QueryableStoreTypes.keyValueStore());
                    log.warn("found....");
                    waitForQueryable.shutdown();
                } catch (Exception e) {
                    log.warn("waiting....");
                }
            }, 0L, 100L, TimeUnit.MILLISECONDS);// wait min
            waitForQueryable.awaitTermination(10, TimeUnit.MINUTES);
        } catch (Exception e) { throw new RuntimeException(e); }
    }
}
