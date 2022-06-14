package com.mikerusoft.playground.simple.streams;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.JsonTimestampExtractor;
import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.models.simple.MyObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils.createProduced;

public class JoinWithWindowStream implements Streamable {
    @Override
    public void runStream(String url) {
        Properties config = KafkaStreamUtils.streamProperties("join-with-window-stream", url, MyObject.class);

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, MyObject> stream1 =
            builder.stream("join-with-window-stream-1",
                Consumed.with(
                    Serdes.String(),
                    new JSONSerde<>(MyObject.class)
                ).withTimestampExtractor(new JsonTimestampExtractor<>(MyObject.class, MyObject::getTimestamp))
            );

        KStream<String, MyObject> stream2 =
            builder.stream("join-with-window-stream-2",
                Consumed.with(
                    Serdes.String(),
                    new JSONSerde<>(MyObject.class)
                ).withTimestampExtractor(new JsonTimestampExtractor<>(MyObject.class, MyObject::getTimestamp))
            );

        stream1.join(stream2, MyObject::mergeWith, JoinWindows.of(Duration.ofSeconds(60)))
        .to("join-result", createProduced(MyObject.class));

        Topology topology = builder.build();
        System.out.println("" + topology.describe());

        KafkaStreamUtils.runStream(new KafkaStreams(topology, config));
    }
}
