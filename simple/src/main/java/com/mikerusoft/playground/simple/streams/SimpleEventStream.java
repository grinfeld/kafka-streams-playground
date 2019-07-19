package com.mikerusoft.playground.simple.streams;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.kafkastreamsinit.SingleFieldSerdeForSerializer;
import com.mikerusoft.playground.models.events.Event;
import com.mikerusoft.playground.models.simple.MyObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.UUID;

// sh kafka-console-consumer --bootstrap-server localhost:9092 --topic events-stream --from-beginning --group tempme
// sh kafka-topics --zookeeper localhost:22181 --topic events-stream --describe

@Component("events-thorough")
public class SimpleEventStream implements Streamable {
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

        KTable<String, Long> ktable = builder2.table("events-stream-time", Consumed.with(Serdes.String(), Serdes.Long()));

        builder2.stream("events-result-stream", Consumed.with(Serdes.String(), new JSONSerde<>(Event.class)))
            .peek((k,ev) -> System.out.println(ev))
            .leftJoin(ktable, (value1, value2) -> value2 != null && value1.getTimestamp() > value2 ? value1 : null)
            .filter((k, e) -> e != null)
            .to("events-after-join", Produced.with(Serdes.String(), new JSONSerde<>(Event.class)));

        Topology topology2 = builder1.build();
        System.out.println("" + topology2.describe());

        KafkaStreamUtils.runStream(new KafkaStreams(topology1, config1), new KafkaStreams(topology2, config2));


    }
}
