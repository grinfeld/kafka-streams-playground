package com.mikerusoft.playground.generatedata;

import com.mikerusoft.playground.kafkastreamsinit.producer.ProducerCreator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("counter_ex_window")
public class CounterForWindowExampleGenerator implements Generator {

    @Value("${broker_url:localhost:9092}")
    private String url;

    @Value("${topic-name:counter-topic}")
    private String topicName;

    @Override
    public void run() throws Exception {
        KafkaProducer<String, Integer> producer = ProducerCreator.createProducer(url, new StringSerializer(), new IntegerSerializer());

        generateRecords(producer, topicName);

        Thread.sleep(7000);
        generateRecords(producer, topicName);

        Thread.sleep(7000);
        generateRecords(producer, topicName);
    }

    private static void generateRecords(KafkaProducer<String, Integer> producer, String topicName) {
        producer.send(new ProducerRecord<>(topicName, "s1", 1), (metadata, exception) -> {});
        producer.send(new ProducerRecord<>(topicName, "s2", 2), (metadata, exception) -> {});
        producer.send(new ProducerRecord<>(topicName, "s1", 1), (metadata, exception) -> {});
        producer.send(new ProducerRecord<>(topicName, "s3", 3), (metadata, exception) -> {});
        producer.send(new ProducerRecord<>(topicName, "s1", 1), (metadata, exception) -> {});
    }
}
