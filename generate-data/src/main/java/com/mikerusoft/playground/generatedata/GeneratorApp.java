package com.mikerusoft.playground.generatedata;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import com.mikerusoft.playground.generatedata.kafka.ProducerCreator;
import com.mikerusoft.playground.models.udhi.UdhiMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@SpringBootApplication
public class GeneratorApp implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(GeneratorApp.class, args);
    }

    private static final Integer MAX_PARTS = 10;

    private static final Random random = new Random();
    private static AtomicInteger counter = new AtomicInteger(1);

    @Value("${broker_url:localhost:9092}")
    private String url;

    @Value("${topic-name:received-messages}")
    private String topicName;

    @Value("${takeWhile:5}")
    private int takeWhile;

    @Override
    public void run(String... args) throws Exception {
        KafkaProducer<String, UdhiMessage> producer = ProducerCreator.createProducer(url, new StringSerializer(), new JSONSerde<UdhiMessage>());
        DataGenerator.generateFlux(i -> (i + 1) % MAX_PARTS)
            .buffer(MAX_PARTS)
            .map(list -> {
                short id = (short) (counter.addAndGet(1) % Short.MAX_VALUE);
                int providerId = random.nextInt(30) + 1;
                List<UdhiMessage> messages = list.stream().map(i -> UdhiMessage.builder()
                        .size(MAX_PARTS.shortValue()).id(id).text("Stammm " + i)
                        .providerId(providerId)
                        .ind(i.shortValue())
                        .build()).collect(Collectors.toList());
                Collections.shuffle(messages);
                return messages;
            }).takeWhile(l -> counter.get() < takeWhile).flatMap(Flux::fromIterable)
        .subscribe(g ->
            producer.send(new ProducerRecord<>(topicName, g.getProviderId() + "_" + g.getId(), g), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //System.out.println();
                }
            })
        )
        ;

    }
}
