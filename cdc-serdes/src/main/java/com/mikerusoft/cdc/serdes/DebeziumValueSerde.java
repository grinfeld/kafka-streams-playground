package com.mikerusoft.cdc.serdes;

import io.debezium.serde.DebeziumSerdes;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Parsing Debezium CDC data
 * Important: the schema part is ignored, means if field appears as Integer in schema,
 * but value and `T` is String - will try to cast as String, ignoring the schema validation
 * @param <T>
 */
public class DebeziumValueSerde<T> implements Serde<T> {

    private static final Map<String, Object> config = new HashMap<>();
    static {
        config.put("unknown.properties.ignored", true);
        config.put("from.field", "after");
    }

    private final Serde<T> innerSerde;

    public DebeziumValueSerde(Class<T> clazz, boolean isKey) {
        this.innerSerde = DebeziumSerdes.payloadJson(clazz);
        this.innerSerde.configure(config, isKey);
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            throw new UnsupportedOperationException("Serialization is not supported for this Serde");
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, data) -> Optional.ofNullable(data).filter(d -> d.length > 0)
                .map(d -> innerSerde.deserializer().deserialize(topic, d))
                .orElse(null);
    }
}
