package com.mikerusoft.playground.udhi;

import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.models.udhi.GroupMessage;
import com.mikerusoft.playground.models.udhi.ReadyMessage;
import com.mikerusoft.playground.models.udhi.UdhiMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils.createProduced;
import static com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils.materializedWindow;

@SpringBootApplication
@Slf4j
public class UdhiOnlyApp implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(UdhiOnlyApp.class, args);
    }

    @Value("${broker_url:localhost:9092}")
    private String url;

    @Override
    public void run(String... args) throws Exception {

        Properties config = KafkaStreamUtils.streamProperties("udhi-app" + UUID.randomUUID().toString(), url, UdhiMessage.class);

        final StreamsBuilder builder = new StreamsBuilder();
            KafkaStreamUtils.createStringJsonStream(UdhiMessage.class, "received-messages", builder)
                .groupByKey()
                .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(10)))
                .aggregate(
                    GroupMessage::new,
                    (k, v, a) -> Utils.addMessageToGroup(v, a),
                    materializedWindow(GroupMessage.class)
                )
                .filter( (k, v) -> v.ready())
                .toStream()
                .filter((w,v) -> v != null)
                .map( (w, v) -> new KeyValue<>(w.key(), v.convert()))
                .to("ready-messages", createProduced(ReadyMessage.class));

        Topology topology = builder.build();
        System.out.println("" + topology.describe());

        KafkaStreamUtils.runStream(new KafkaStreams(topology, config));
    }
}
