package com.mikerusoft.playground.kafkastreamsinit;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.function.Function;

@NoArgsConstructor
@AllArgsConstructor
@Builder(builderClassName = "Builder", toBuilder = true)
public class SingleFieldSerdeForSerializer<F, T> implements Serde<F> {

    private Serializer<T> singleFieldSerializer;
    private Deserializer<T> singleFieldDeSerializer;
    private Function<F, T> fieldExtractor;
    private Function<T, F> fieldSetter;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public void close() { singleFieldSerializer.close(); }

    @Override
    public Serializer<F> serializer() {
        return new Serializer<F>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {}

            @Override
            public byte[] serialize(String topic, F data) {
                return singleFieldSerializer.serialize(topic, fieldExtractor.apply(data));
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public Deserializer<F> deserializer() {
        return new Deserializer<F>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {}

            @Override
            public F deserialize(String topic, byte[] data) {
                return fieldSetter.apply(singleFieldDeSerializer.deserialize(topic, data));
            }

            @Override
            public void close() {}
        };
    }
}
