package com.mikerusoft.playground.simple.streams;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.JsonTimestampExtractor;
import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.models.simple.Counter;
import com.mikerusoft.playground.models.simple.MyObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils.createProduced;

@Slf4j
@Component("static")
public class StaticWindowStream implements Streamable {

    @Value("${windowDurationSec:120}")
    private int windowDurationSec;

    public void runStream(String url) {
        Properties config = KafkaStreamUtils.streamProperties("window-stream", url, MyObject.class);
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, MyObject> stream =
            builder.stream("window-stream-1",
                Consumed.with(
                    Serdes.String(),
                    new JSONSerde<>(MyObject.class)
                ).withTimestampExtractor(new JsonTimestampExtractor<>(MyObject.class, MyObject::getTimestamp))
            );

        stream
            .peek(((key, value) -> log.info("received {} -> {}", key, value)))
            .groupByKey()
            .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(windowDurationSec)))
            .aggregate(Counter::new, (k, v, a) -> a.op(1),
                    Materialized.with(Serdes.String(), new JSONSerde<>(Counter.class)))
            .toStream()
            .peek((key, value) -> log.info("Window start at {} end at {} with key {} and data {}",
                new Date(key.window().start()),
                new Date(key.window().end()),
                key.key(),
                value
            ))
                .map((key, value) -> new KeyValue<>(getStingKeyForWindow(key), value))
                .to("window-stream-1-result", createProduced(Counter.class));
        Topology topology = builder.build();
        System.out.println("" + topology.describe());

        KafkaStreamUtils.runStream(new KafkaStreams(topology, config));
    }

    private static String getStingKeyForWindow(Windowed<String> key) {
        return key.window().start() + "_" + key.window().end() + "_" + key.key();
    }


}
