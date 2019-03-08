package com.mikerusoft.playground.kafkastreamsinit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.function.Function;

public class JsonTimestampExtractor<V> implements TimestampExtractor {

    private Function<V, Long> timeExtraction;
    protected Class<V> valueClass;

    // need the empty one if using it inside the properties - maybe useless
    // for such use (should be in use by derived classes with specified Class<V> valueClass)
    protected JsonTimestampExtractor() {
    }

    public JsonTimestampExtractor(Class<V> valueClass, Function<V, Long> timeExtraction) {
        this.timeExtraction = timeExtraction;
        this.valueClass = valueClass;
    }

    // TODO: when this one is called - before serialization or after ????

    @Override
    @SuppressWarnings("unchecked")
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        if (timeExtraction == null)
            throw new IllegalArgumentException("No time extraction function for " + record.value());
        if (record.value() != null && valueClass.isAssignableFrom(record.value().getClass())) {
            return timeExtraction.apply((V)record.value());
        }

        throw new IllegalArgumentException("JsonTimestampExtractor cannot recognize the record value " + record.value());
    }
}
