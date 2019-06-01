package com.mikerusoft.playground.generatedata;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.producer.ProducerCreator;
import com.mikerusoft.playground.models.simple.ErrorData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.UUID;

@Component("errorData")
public class ErrorDataGenerator implements Generator {

    private static final Random random = new Random();

    @Value("${broker_url:localhost:9092}")
    private String url;

    @Value("${topic-name:MAIL_RELAY_STATUS}")
    private String topicName;

    @Override
    public void run() throws Exception {
        KafkaProducer<String, ErrorData> producer = ProducerCreator.createProducer(url, new StringSerializer(), new JSONSerde<>());
        DataGenerator.generateFlux(i -> (i + 1))
            .map(i -> ErrorData.builder().mimeMessageId(UUID.randomUUID().toString()).recipients("gsdgsdgsdg").errorMessage("error").state("bounce").build())
            .take(1)
            .subscribe(e ->
                producer.send(new ProducerRecord<>(topicName, null, e), (metadata, exception) -> {})
            );
    }

}
