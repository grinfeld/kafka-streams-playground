package com.mikerusoft.playground.generatedata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mikerusoft.playground.kafkastreamsinit.producer.ProducerCreator;
import com.mikerusoft.playground.models.monitoring.MessageStatus;
import com.mikerusoft.playground.models.monitoring.ReceivedMessage;
import com.mikerusoft.playground.models.monitoring.SentMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;

@Component("monitor")
public class MonitorMessagingGenerator implements Generator {

    private static final Random random = new Random();

    @Value("${broker_url:localhost:9092}")
    private String url;

    @Value("${topic1-name:received-messages}")
    private String topicName1;
    @Value("${topic2-name:sent-messages}")
    private String topicName2;
    @Value("${topic3-name:status-messages}")
    private String topicName3;

    @Value("${takeWhile:10}")
    private int takeWhile;

    @Autowired
    private ObjectMapper mapper;

    public void run() throws Exception {
        KafkaProducer<String, byte[]> producer = ProducerCreator.createProducer(url, new StringSerializer(), new ByteArraySerializer());
        DataGenerator.generateFlux(i -> i + 1)
                .takeWhile(i -> i < takeWhile)
                .map(i ->
                    ReceivedMessage.builder()
                        .text("Text " + i)
                        .id(UUID.randomUUID().toString())
                        .receivedTime(System.currentTimeMillis())
                        .from("97254440" + i)
                        .to("97254430" + i)
                        .order(i)
                        .build()
                )
                .buffer()
                .map(l -> {
                    Collections.shuffle(l);
                    return new ArrayList<>(l);
                })
                .flatMap(Flux::fromIterable)
                .doOnNext(r -> producer.send(new ProducerRecord<String, byte[]>(topicName1, r.getId(), serialize(r))))
                .filter(r -> r.getOrder() % 4 != 0)
                .map(r ->
                    SentMessage.builder()
                        .order(r.getOrder())
                        .extMessageId(UUID.randomUUID().toString())
                        .from(r.getFrom())
                        .to(r.getTo())
                        .id(r.getId())
                        .providerId(String.valueOf(random.nextInt(120)))
                        .sentTime(r.getReceivedTime() + 10)
                        .status("SENT")
                        .statusTime(r.getReceivedTime() + 15)
                        .build()
                )
                .buffer()
                .map(l -> {
                    Collections.shuffle(l);
                    return new ArrayList<>(l);
                })
                .flatMap(Flux::fromIterable)
                .doOnNext(s -> producer.send(new ProducerRecord<String, byte[]>(topicName2, s.getId(), serialize(s))))
                .filter(s -> s.getOrder() % 6 != 0)
                .map(s ->
                    MessageStatus.builder()
                        .id(s.getId())
                        .providerId(s.getProviderId())
                        .extMessageId(s.getExtMessageId())
                        .from(s.getFrom())
                        .to(s.getTo())
                        .status("DELIVERED")
                        .statusTime(s.getStatusTime() + 15)
                        .build()
                ).subscribe(s -> producer.send(new ProducerRecord<String, byte[]>(topicName3, s.getId(), serialize(s))))
                ;
    }

    private byte[] serialize(Object o) {
        try {
            return mapper.writeValueAsBytes(o);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
