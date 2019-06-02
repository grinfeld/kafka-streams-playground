package com.mikerusoft.playground.simple.streams;

import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component("static-window-counter-app")
public class StaticWindowCountStream implements Streamable {

    @Value("${windowDurationSec:5}")
    private int windowDurationSec;

    @Override
    public void runStream(String url) {
        Properties config = KafkaStreamUtils.streamProperties("counter-stream2", url, Integer.class);
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("counter-topic", Consumed.with(Serdes.String(), Serdes.Integer()))
            .peek(((key, value) -> log.info("received {} -> {}", key, value)))
            .groupByKey(Serialized.with(Serdes.String(), Serdes.Integer()))
            .windowedBy(SessionWindows.with(TimeUnit.SECONDS.toMillis(5)))
            .count()
            // (Windowed<String>, Long)
            .toStream((key, value) -> {
                log.info("{} - {}", new Date(key.window().start()), new Date(key.window().end()));
                return key.key();
            })
            .filter((k,v) -> v > 1)
            .peek((k,v) -> log.info("produce value {} for key {}", v, k))
            .to("counter-topic-to", Produced.with(Serdes.String(), Serdes.Long()));
        Topology topology = builder.build();
        System.out.println("" + topology.describe());

        KafkaStreamUtils.runStream(new KafkaStreams(topology, config));
    }
}
