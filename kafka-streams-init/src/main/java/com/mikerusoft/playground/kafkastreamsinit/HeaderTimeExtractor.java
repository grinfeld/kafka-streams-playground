package com.mikerusoft.playground.kafkastreamsinit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.nio.ByteBuffer;

public class HeaderTimeExtractor implements TimestampExtractor {

    private String headerTimeKey;

    public HeaderTimeExtractor(String headerTimeKey) {
        this.headerTimeKey = headerTimeKey;
    }

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        return ByteBuffer.wrap(record.headers().lastHeader(headerTimeKey).value()).getLong();
    }
}
