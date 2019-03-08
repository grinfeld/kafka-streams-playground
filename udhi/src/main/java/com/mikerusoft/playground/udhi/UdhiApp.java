package com.mikerusoft.playground.udhi;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.models.udhi.GroupMessage;
import com.mikerusoft.playground.models.udhi.ReadyMessage;
import com.mikerusoft.playground.models.udhi.UdhiMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@Slf4j
public class UdhiApp implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(UdhiApp.class, args);
    }

    @Value("${broker_url:localhost:9092}")
    private String url;

    @Override
    public void run(String... args) throws Exception {

        Properties config = KafkaStreamUtils.streamProperties("udhi-app" + UUID.randomUUID().toString(), url, UdhiMessage.class);

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, UdhiMessage> stream =
            KafkaStreamUtils.createStringJsonStream(UdhiMessage.class, "received-messages", builder)
                ;

        KStream<String, UdhiMessage>[] branch = stream.branch((s, m) -> !m.udhi(), (s, m) -> m.udhi());

        KStream<String, UdhiMessage> nonUdhiStream = branch[0];
        nonUdhiStream
                .mapValues(m -> ReadyMessage.builder().fullMessage(true).id(m.getId())
                        .text(m.getText())
                        .providerId(m.getProviderId()).build())
                .to("ready-messages", Produced.with(Serdes.String(), new JSONSerde<>(ReadyMessage.class)));

        KStream<String, UdhiMessage> udhiStream = branch[1];
        udhiStream
                .groupByKey()
                .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(10)))
                .aggregate(GroupMessage::new, (k, v, a) -> {if (a.getSize() == 0) { a.setSize(v.getSize());} a.add(v); return a;}, Materialized.with(Serdes.String(), new JSONSerde<>(GroupMessage.class)))
                .filter( (k, v) -> v.ready())
                .toStream()
                .filter((w,v) -> v != null)
                .map( (w, v) -> new KeyValue<>(w.key(), v.convert()))
                .to("ready-messages", Produced.with(Serdes.String(), new JSONSerde<>(ReadyMessage.class)));

        Topology topology = builder.build();
        System.out.println("" + topology.describe());

        KafkaStreamUtils.runStream(new KafkaStreams(topology, config));
    }
}
