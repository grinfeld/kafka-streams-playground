package com.mikerusoft.playground.message.monitoring;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.JsonTimestampExtractor;
import com.mikerusoft.playground.kafkastreamsinit.KafkaStreamUtils;
import com.mikerusoft.playground.models.monitoring.MessageForStatus;
import com.mikerusoft.playground.models.monitoring.MessageStatus;
import com.mikerusoft.playground.models.monitoring.SentMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Properties;

@Slf4j
@SpringBootApplication
public class DrApp implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(DrApp.class, args);
    }

    @Value("${broker_url:localhost:9092}")
    private String url;

    @Override
    public void run(String... args) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();

        Properties config = KafkaStreamUtils.streamProperties("dr-app", url, SentMessage.class);

        KStream<String, SentMessage> sentMessageStreamBuilder = builder.stream("sent-messages",
            Consumed.with(Serdes.String(), new JSONSerde<>(SentMessage.class))
                    .withTimestampExtractor(new JsonTimestampExtractor<>(SentMessage.class, SentMessage::getSentTime))
        )
        .peek((k,v) -> {
            log.info("received message {}", v);
        })
        .selectKey(DrApp::createKey);

        KStream<String, MessageStatus> statusMessageStreamBuilder = builder.stream("status-messages",
            Consumed.with(Serdes.String(), new JSONSerde<>(MessageStatus.class))
                    .withTimestampExtractor(new JsonTimestampExtractor<>(MessageStatus.class, MessageStatus::getStatusTime))
        )
        .peek((k,v) -> {
            log.info("received status {}", v);
        })
        .selectKey(DrApp::createKey);

        KStream<String, MessageStatus> joinedStream = sentMessageStreamBuilder.join(
            statusMessageStreamBuilder,
            (sent, dr) -> {
                if (sent.getSentTime() > dr.getStatusTime()) {
                    log.info("Something went wrong");
                    // ignore, add alert or something else
                }
                return dr.toBuilder().id(sent.getId()).build();
            },
            JoinWindows.of(Duration.ofSeconds(60L)),
            StreamJoined.with(Serdes.String(), new JSONSerde<>(SentMessage.class), new JSONSerde<>(MessageStatus.class))
        );

        joinedStream
            //.filter((k, v) -> v != null)
            .peek((key, status) -> {
                log.info("received final status for {}", status);
            })
        .to("final-status", Produced.with(Serdes.String(), new JSONSerde<>(MessageStatus.class)));

        Topology topology = builder.build();
        System.out.println("" + topology.describe());

        KafkaStreamUtils.runStream(new KafkaStreams(topology, config));
    }

    private static String createKey(String oldKey, MessageForStatus value) {
        return value.getProviderId() + value.getExtMessageId() + value.getFrom() + value.getTo();
    }
}

/*
logs from running app:

message SentMessage(id=1d51a90a-fb90-4988-9636-141d43ba5865, providerId=119, extMessageId=cc3e48f6-e641-43d6-a30e-2bbd1a33bc02, from=972544406, to=972544306, status=SENT, statusTime=1559893509893, sentTime=1559893509888, order=6)
message SentMessage(id=4922c6dc-c3f3-44ee-b0c9-e66fa71e39e6, providerId=31, extMessageId=16f409b4-7dac-4625-9730-80a3523a5962, from=972544403, to=972544303, status=SENT, statusTime=1559893509893, sentTime=1559893509888, order=3)
message SentMessage(id=aa501317-a1c1-43e5-92c4-c0549b9a30df, providerId=63, extMessageId=17f54e6f-df15-45ee-859f-48029a3d81d5, from=972544407, to=972544307, status=SENT, statusTime=1559893509893, sentTime=1559893509888, order=7)
message SentMessage(id=9aed49ff-ceb4-4f80-a3f9-95a6e35400fb, providerId=7, extMessageId=721cae9a-b102-4a05-bf32-035e10ce098f, from=972544402, to=972544302, status=SENT, statusTime=1559893509893, sentTime=1559893509888, order=2)
message SentMessage(id=175f47bc-9fd5-49a5-bfa6-52c66253e3d0, providerId=44, extMessageId=d2a8e6d2-e0db-44b2-b5b8-08ab3f235010, from=972544405, to=972544305, status=SENT, statusTime=1559893509893, sentTime=1559893509888, order=5)
message SentMessage(id=ed78978b-f719-4699-9d70-5673f57ba59d, providerId=45, extMessageId=1dfbbfeb-baa7-445a-8c5d-f950bd051c95, from=972544401, to=972544301, status=SENT, statusTime=1559893509893, sentTime=1559893509888, order=1)
message SentMessage(id=8653b530-ac66-46a4-aaf4-fe3140addcd2, providerId=113, extMessageId=592f22ed-eb14-4015-be08-8614a44768e8, from=972544409, to=972544309, status=SENT, statusTime=1559893509893, sentTime=1559893509888, order=9)

status MessageStatus(id=4922c6dc-c3f3-44ee-b0c9-e66fa71e39e6, providerId=31, from=972544403, to=972544303, extMessageId=16f409b4-7dac-4625-9730-80a3523a5962, status=DELIVERED, statusTime=1559893509908)
status MessageStatus(id=aa501317-a1c1-43e5-92c4-c0549b9a30df, providerId=63, from=972544407, to=972544307, extMessageId=17f54e6f-df15-45ee-859f-48029a3d81d5, status=DELIVERED, statusTime=1559893509908)
status MessageStatus(id=9aed49ff-ceb4-4f80-a3f9-95a6e35400fb, providerId=7, from=972544402, to=972544302, extMessageId=721cae9a-b102-4a05-bf32-035e10ce098f, status=DELIVERED, statusTime=1559893509908)
status MessageStatus(id=175f47bc-9fd5-49a5-bfa6-52c66253e3d0, providerId=44, from=972544405, to=972544305, extMessageId=d2a8e6d2-e0db-44b2-b5b8-08ab3f235010, status=DELIVERED, statusTime=1559893509908)
status MessageStatus(id=ed78978b-f719-4699-9d70-5673f57ba59d, providerId=45, from=972544401, to=972544301, extMessageId=1dfbbfeb-baa7-445a-8c5d-f950bd051c95, status=DELIVERED, statusTime=1559893509908)
status MessageStatus(id=8653b530-ac66-46a4-aaf4-fe3140addcd2, providerId=113, from=972544409, to=972544309, extMessageId=592f22ed-eb14-4015-be08-8614a44768e8, status=DELIVERED, statusTime=1559893509908)

final status for MessageStatus(id=4922c6dc-c3f3-44ee-b0c9-e66fa71e39e6, providerId=31, from=972544403, to=972544303, extMessageId=16f409b4-7dac-4625-9730-80a3523a5962, status=DELIVERED, statusTime=1559893509908)
final status for MessageStatus(id=aa501317-a1c1-43e5-92c4-c0549b9a30df, providerId=63, from=972544407, to=972544307, extMessageId=17f54e6f-df15-45ee-859f-48029a3d81d5, status=DELIVERED, statusTime=1559893509908)
final status for MessageStatus(id=9aed49ff-ceb4-4f80-a3f9-95a6e35400fb, providerId=7, from=972544402, to=972544302, extMessageId=721cae9a-b102-4a05-bf32-035e10ce098f, status=DELIVERED, statusTime=1559893509908)
final status for MessageStatus(id=175f47bc-9fd5-49a5-bfa6-52c66253e3d0, providerId=44, from=972544405, to=972544305, extMessageId=d2a8e6d2-e0db-44b2-b5b8-08ab3f235010, status=DELIVERED, statusTime=1559893509908)
final status for MessageStatus(id=ed78978b-f719-4699-9d70-5673f57ba59d, providerId=45, from=972544401, to=972544301, extMessageId=1dfbbfeb-baa7-445a-8c5d-f950bd051c95, status=DELIVERED, statusTime=1559893509908)
final status for MessageStatus(id=8653b530-ac66-46a4-aaf4-fe3140addcd2, providerId=113, from=972544409, to=972544309, extMessageId=592f22ed-eb14-4015-be08-8614a44768e8, status=DELIVERED, statusTime=1559893509908)
*/