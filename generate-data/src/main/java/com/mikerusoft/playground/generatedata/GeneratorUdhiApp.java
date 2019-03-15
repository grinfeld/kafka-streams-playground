package com.mikerusoft.playground.generatedata;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import com.mikerusoft.playground.kafkastreamsinit.producer.ProducerCreator;
import com.mikerusoft.playground.models.udhi.UdhiMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Component("udhi")
public class GeneratorUdhiApp implements Generator {

    private static final Random random = new Random();
    private static AtomicInteger counter = new AtomicInteger(1);

    @Value("${broker_url:localhost:9092}")
    private String url;

    @Value("${topic-name:received-messages}")
    private String topicName;

    @Value("${takeWhile:10}")
    private int takeWhile;

    @Value("${maxParts:10}")
    private Integer maxParts;

    public void run() throws Exception {
        KafkaProducer<String, UdhiMessage> producer = ProducerCreator.createProducer(url, new StringSerializer(), new JSONSerde<>());
        DataGenerator.generateFlux(i -> (i + 1) % maxParts)
            .buffer(maxParts)
            .map(list -> {
                short id = (short) (counter.addAndGet(1) % Short.MAX_VALUE);
                int providerId = random.nextInt(30) + 1;
                final Integer maxParts = providerId % 6 == 0 ? this.maxParts + 1 : this.maxParts;
                List<UdhiMessage> messages = list.stream().map(i -> UdhiMessage.builder()
                    .size(maxParts.shortValue()).id(id).text("Stammm " + i)
                    .providerId(providerId)
                    .sentTime(System.currentTimeMillis() + random.nextInt(10))
                    .ind(i.shortValue())
                    //.ind(this.maxParts.shortValue())
                    .build()).collect(Collectors.toList());
                Collections.shuffle(messages);
                return messages;
            }).takeWhile(l -> counter.get() <= takeWhile).flatMap(Flux::fromIterable)
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
