package com.mikerusoft.playground.simple.streams;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.models.events.Event;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Value;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component("ktable-stream")
public class QueryKTableStream implements Streamable {

    @Override
    public void runStream(String url) {

        Properties config = KafkaStreamUtils.streamProperties("ktable-stream-example", url, Event.class);

        final StreamsBuilder builder = new StreamsBuilder();
        // let's assume <SectionId, Event>
        KStream<String, Event> eventStream = builder.stream("events-stream", Consumed.with(Serdes.String(), new JSONSerde<>(Event.class)));

        // we have topic with pvs per sectionId (default retention is 24 hours)
        KTable<String, Long> sectionToPvc = builder.table("section-pvc", Consumed.with(Serdes.String(), Serdes.Long()));

        // eventStream.join(sectionToPvc, Tuple::of)
        eventStream.join(sectionToPvc, (event, time) -> Tuple.of(event, time))
            .filter((key, tuple) -> tuple.getEvent() != null && tuple.getTime() != null)
            .filter((key, tuple) -> tuple.getEvent().getTimestamp() > tuple.getTime())
            .mapValues(tuple -> tuple.getEvent())
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
