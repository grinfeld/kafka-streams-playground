package com.mikerusoft.playground.message.monitoring;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.JsonTimestampExtractor;
import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.models.monitoring.MessageMonitor;
import com.mikerusoft.playground.models.monitoring.MessageStatus;
import com.mikerusoft.playground.models.monitoring.ReceivedMessage;
import com.mikerusoft.playground.models.monitoring.SentMessage;
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
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class MonitoringApp implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(MonitoringApp.class, args);
    }

    @Value("${broker_url:localhost:9092}")
    private String url;

    @Override
    public void run(String... args) throws Exception {
        Properties config = KafkaStreamUtils.streamProperties("monitoring-app", url, ReceivedMessage.class);

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ReceivedMessage> receivedMessagesStream =
                builder.stream("received-messages",
                Consumed.with(Serdes.String(), new JSONSerde<>(ReceivedMessage.class)
            ).withTimestampExtractor(new JsonTimestampExtractor<>(ReceivedMessage.class, ReceivedMessage::getReceivedTime))
        );

        KStream<String, SentMessage> sentMessagesStream =
            builder.stream("sent-messages",
                Consumed.with(Serdes.String(), new JSONSerde<>(SentMessage.class)
                ).withTimestampExtractor(new JsonTimestampExtractor<>(SentMessage.class, SentMessage::getSentTime))
            );


        KStream<String, MessageStatus> statusMessagesStream =
            builder.stream("status-messages",
                Consumed.with(Serdes.String(), new JSONSerde<>(MessageStatus.class)
                ).withTimestampExtractor(new JsonTimestampExtractor<>(MessageStatus.class, MessageStatus::getStatusTime))
            );

        receivedMessagesStream
                .mapValues(v -> MessageMonitor.builder()
                    .receivedTime(v.getReceivedTime())
                    .id(v.getId())
                    .build())
        .leftJoin(sentMessagesStream, (r, s) -> s != null ? r.toBuilder()
                .id(s.getId())
                .extMessageId(s.getExtMessageId())
                .sentTime(s.getSentTime())
                .providerId(s.getProviderId())
                .sentStatusTime(s.getStatusTime())
                .sentStatus(s.getStatus())
                .build() : r.toBuilder().build(),
            JoinWindows.of(TimeUnit.SECONDS.toMillis(60)),
            Joined.with(Serdes.String(),
                new JSONSerde<>(MessageMonitor.class), new JSONSerde<>(SentMessage.class))
        ).leftJoin(statusMessagesStream, (st, m) -> m != null ? st.toBuilder()
                .id(m.getId())
                .drStatus(m.getStatus())
                .extMessageId(m.getExtMessageId())
                .providerId(m.getProviderId())
                .drStatusTime(m.getStatusTime())
                .build() : st.toBuilder().build(),
            JoinWindows.of(TimeUnit.SECONDS.toMillis(60)), // todo: check what it relates to -> start of stream, previous join or event emitted from stream
            Joined.with(Serdes.String(),
                new JSONSerde<>(MessageMonitor.class), new JSONSerde<>(MessageStatus.class))
        )
        .groupByKey()
        .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(120)))
        .aggregate(
            () -> MessageMonitor.builder().build(),
            (k, v, a) -> a.merge(v),
            Materialized.with(Serdes.String(), new JSONSerde<>(MessageMonitor.class))
        )
        .toStream()
        .filter((k, m) -> m.getDrStatus() == null || m.getDrStatus() == null)
        .map((key, v) -> new KeyValue<>(key.key(), v))
        .to("message-monitoring", Produced.with(Serdes.String(), new JSONSerde<>(MessageMonitor.class)));

        Topology topology = builder.build();
        System.out.println("" + topology.describe());

        KafkaStreamUtils.runStream(new KafkaStreams(topology, config));
    }
}
