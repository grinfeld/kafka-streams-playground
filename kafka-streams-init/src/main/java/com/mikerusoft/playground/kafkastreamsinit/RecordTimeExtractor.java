package com.mikerusoft.playground.kafkastreamsinit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class RecordTimeExtractor implements TimestampExtractor {
    @Override
    // default behavior - no really need this one
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        return record.timestamp();
    }
}
