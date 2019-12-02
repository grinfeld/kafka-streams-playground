package com.mikerusoft.playground.simple.streams;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.models.events.Event;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component("ktable-stream2")
public class QueryKTableV2Stream implements Streamable {

    @Override
    public void runStream(String url) {

        Properties config = KafkaStreamUtils.streamProperties("ktable-stream-ex" + UUID.randomUUID().toString(), url, Event.class);

        final StreamsBuilder builder = new StreamsBuilder();
        // let's assume <SectionId, Event>
        KStream<String, Event> eventStream = builder.stream("events-stream", Consumed.with(Serdes.String(), new JSONSerde<>(Event.class)));

        KTable<String, Long> sectionToPvc = eventStream.groupByKey().reduce((value1, value2) -> value2)
                .filter((id, event) -> event.getType().equals("pageView")).mapValues(Event::getTimestamp);

        eventStream.leftJoin(sectionToPvc, Tuple::of)
                .peek((key, event) -> log.info("" + event))
                .filter((key, tuple) -> tuple.getEvent() != null)
                .filter((key, tuple) -> tuple.getTime() == null || tuple.getEvent().getTimestamp() >= tuple.getTime() + TimeUnit.SECONDS.toMillis(2))
                .mapValues(tuple -> tuple.getEvent())
                .peek((key, event) -> log.info("" + event))
                .to("events-for-only-active-sections")
        ;

        Topology mainStreamTopology = builder.build();
        System.out.println("" + mainStreamTopology.describe());

        KafkaStreamUtils.runStream(
                new KafkaStreams(mainStreamTopology, config)
        );
    }

    private static Materialized<String, Long, KeyValueStore<Bytes, byte[]>> defineAggregateStore(String supplier) {
        return Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(supplier)
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()).withCachingDisabled();
    }

    @Data
    @Value
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    static class Tuple<L,R> {

        private static <L,R> Tuple<L, R> of(L left, R right) {
            return new Tuple<>(left, right);
        }

        private L event;
        private R time;
    }

}
