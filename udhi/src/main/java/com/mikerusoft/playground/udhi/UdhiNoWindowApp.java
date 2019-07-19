package com.mikerusoft.playground.udhi;

import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.kafkastreamsinit.SingleFieldSerdeForSerializer;
import com.mikerusoft.playground.models.udhi.GroupMessage;
import com.mikerusoft.playground.models.udhi.ReadyMessage;
import com.mikerusoft.playground.models.udhi.UdhiMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;
import java.util.UUID;

import static com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils.createProduced;
import static com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils.materialized;

@SpringBootApplication
@Slf4j
public class UdhiNoWindowApp implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(UdhiNoWindowApp.class, args);
    }

    @Value("${broker_url:localhost:9092}")
    private String url;

    @Override
    public void run(String... args) throws Exception {

        Properties config = KafkaStreamUtils.streamProperties("udhi-app" + UUID.randomUUID().toString(), url, UdhiMessage.class);

        final StreamsBuilder builder = new StreamsBuilder();
            KafkaStreamUtils.createStringJsonStream(UdhiMessage.class, "received-messages", builder)
                .groupByKey()
                .aggregate(
                    GroupMessage::new,
                    (k, v, a) -> Utils.addMessageToGroup(v, a),
                    materialized(GroupMessage.class)
                )
                .filter( (k, v) -> v.ready())
                .toStream()
                .filter((k,v) -> v != null)
                .mapValues(GroupMessage::convert)
                .through("temp-topic-for-time",
                    Produced.valueSerde(new SingleFieldSerdeForSerializer<>(Serdes.Long().serializer(), ReadyMessage::getSentTime)))
                .to("ready-messages", createProduced(ReadyMessage.class));

        Topology topology = builder.build();
        System.out.println("" + topology.describe());

        KafkaStreamUtils.runStream(new KafkaStreams(topology, config));
    }
}
