package com.mikerusoft.playground.simple.controllers;

import com.mikerusoft.playground.kafkastreamsinit.JSONSerde;
import com.mikerusoft.playground.kafkastreamsinit.producer.ProducerCreator;
import com.mikerusoft.playground.models.simple.MyObject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.concurrent.ExecutionException;

@Slf4j
@RestController
public class MyController {

    private KafkaProducer<String, MyObject> producer;

    public MyController(@Value("${broker_url:localhost:9092}") String url) {
        this.producer = ProducerCreator.createProducer(url, new StringSerializer(), new JSONSerde<>(MyObject.class));
    }

    @PostMapping("/produce/{topicName}")
    public Response putIn(@PathVariable("topicName") String topicName, @RequestBody MyObject data) throws ExecutionException, InterruptedException {
        if (data == null)
            throw new NullPointerException("Data shouldn't be null");
        Date date = null;
        if (data.getTimestamp() > 0) {
            date = new Date(data.getTimestamp());
        }
        log.info("Received data {} for {}", data, String.valueOf(date));


        RecordMetadata record = producer.send(new ProducerRecord<>(topicName, data.getKey(), data)).get();
        return Response.builder()
                .offset(record.offset())
                .partition(record.partition())
                .topic(record.topic())
                .time(new Date(record.timestamp()))
                .build();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder(builderClassName = "Builder", toBuilder = true)
    private static class Response {
        private long offset;
        private int partition;
        private String topic;
        private Date time;
    }

}
