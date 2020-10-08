package com.mikerusoft.playground.generatedata;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.producer.ProducerCreator;
import com.mikerusoft.playground.models.events.Event;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.Date;
import java.util.Random;

@Component("events")
public class EventGenerator implements Generator {

    @Value("${broker_url:localhost:9092}")
    private String url;

    @Value("${topic-name:events-stream}")
    private String topicName;

    @Value("${takeWhile:20}")
    private int takeWhile;

    @Override
    public void run() throws Exception {
        KafkaProducer<String, Event> producer = ProducerCreator.createProducer(url, new StringSerializer(), new JSONSerde<>());

        DataGenerator.generateFlux(i -> (i + 1))
            .map(EventGenerator::generateEvent)
            .buffer(5)
            .map(l -> {Collections.shuffle(l); return l;})
            .flatMap(Flux::fromIterable)
            .take(takeWhile)
        .subscribe(g -> {
            producer.send(
                new ProducerRecord<>(topicName, g.getId(), g),
                (metadata, exception) -> System.out.println()
            );
        });
    }

    private static Event generateEvent(Integer i) {
        long timestamp = System.currentTimeMillis();
        String tsStr = String.valueOf(timestamp);
        tsStr = tsStr.substring(tsStr.length() - 3);
        Date now = new Date(timestamp);
        return Event.builder().id("key" + i%5).data("Data: " + now.toString() + ", " + tsStr)
            .type(types[random.nextInt() % types.length])
            .timestamp(timestamp).build();
    }

    private static Random random = new Random();
    private static String[] types = new String[] {"pageView", "addCart", "Purchase"};
}
