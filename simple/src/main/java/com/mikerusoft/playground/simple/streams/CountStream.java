package com.mikerusoft.playground.simple.streams;

import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@Component("counter-app")
public class CountStream implements Streamable {
    @Override
    public void runStream(String url) {
        Properties config = KafkaStreamUtils.streamProperties("counter-stream", url, Integer.class);
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("counter-topic", Consumed.with(Serdes.String(), Serdes.Integer()))
                .peek((k,v) -> log.info("get event {} with key {}", v, k))
                .groupByKey(Serialized.with(Serdes.String(), Serdes.Integer()))
                .count()
                .toStream()
                .filter((k,v) -> v > 1)
                .peek((k,v) -> log.info("produce value {} with key {}", v, k))
                //.foreach((k,v) -> { /* write to DB, send Http and etc */})
                .to("counter-topic-to", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        System.out.println("" + topology.describe());

        KafkaStreamUtils.runStream(new KafkaStreams(topology, config));
    }
}
