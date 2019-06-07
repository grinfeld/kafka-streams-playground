package com.mikerusoft.playground.generatedata;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.producer.ProducerCreator;
import com.mikerusoft.playground.models.simple.MyObject;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.Random;

public class MyObjectGenerator implements Generator {

    @Value("${broker_url:localhost:9092}")
    private String url;

    @Value("${topic-name:join-with-window-stream}")
    private String topicName;

    @Value("${takeWhile:10}")
    private int takeWhile;

    private static final Random random = new Random();

    @Override
    public void run() throws Exception {
        KafkaProducer<String, MyObject> producer = ProducerCreator.createProducer(url, new StringSerializer(), new JSONSerde<>());

        DataGenerator.generateFlux(i -> (i + 1))
            .map(i -> MyObject.builder().key("key" + i).value(random.nextInt(25) + "")
                .timestamp(System.currentTimeMillis()).build()
            )
            .buffer(5)
            .map(l -> {Collections.shuffle(l); return l;})
            .flatMap(Flux::fromIterable)
            .take(takeWhile)
        .subscribe(g -> {
            producer.send(new ProducerRecord<>(topicName + "-1", g.getKey(), g), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //System.out.println();
                }
            });
            producer.send(new ProducerRecord<>(topicName + "-2", g.getKey(), g), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //System.out.println();
                }
            });
        });
    }
}
