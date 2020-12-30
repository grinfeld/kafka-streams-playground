package com.mikerusoft.cdc.serdes;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

/**
 * Serde for Kafka Streams to deserialize the Debezium's CDC structure.
 * Used Debezium's JsonSerde as source (by simplifying and changing it slightly)
 * with encapsulated Envelope parsing and returning the expected object by extracting "after" field.
 * The main reason to do this - is not wishing to take the whole debezium-core as dependency inside our project
 * (all credits to Debezium project, original JsonSerde https://github.com/debezium/debezium/blob/master/debezium-core/src/main/java/io/debezium/serde/json/JsonSerde.java)
 * See Debezium Envelope structure examples in `DebeziumJsonSerdeTest` in this project
 * @param <T>
 */
public class DebeziumJsonSerde<T> implements Serde<T> {
    private static final String PAYLOAD_FIELD = "payload";

    private static final String OP_COMMAND_TAG = "op";
    private static final String DATA_TAG_AFTER_CHANGE = "after";
    private static final String create_op = "c";
    private static final String delete_op = "d";
    private static final String update_op = "u";

    private final ObjectMapper mapper;
    private final boolean isKey;
    private final Class<T> clazz;

    public DebeziumJsonSerde(ObjectMapper mapper, Class<T> clazz, boolean isKey) {
        this.mapper = Optional.ofNullable(mapper).orElseGet(ObjectMapper::new);
        this.isKey = isKey;
        this.clazz = clazz;
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            throw new UnsupportedOperationException("Serializing not supported for this Serde");
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return new JsonDeserializer<T>();
    }

    private final class JsonDeserializer<V> implements Deserializer<V> {

        @Override
        public V deserialize(String topic, byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            try {
                JsonNode node = mapper.readTree(data);
                return isKey ? readKey(node) : readValue(node);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private V readValue(JsonNode node) throws IOException {
            JsonNode payload = node.get(PAYLOAD_FIELD);

            // Schema + payload format
            if (payload != null) {
                node = payload;
            }

            // Debezium envelope
            ObjectReader reader = mapper.readerFor(clazz)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            String op = node.get(OP_COMMAND_TAG).asText();
            V data = reader.readValue(node.get(DATA_TAG_AFTER_CHANGE));

            if (op == null)
                return data;

            switch (op) {
                case delete_op:
                    return null;
                case update_op:
                case create_op:
                default:
                    return data;
            }
        }

        private V readKey(JsonNode node) throws IOException {
            ObjectReader reader = mapper.readerFor(clazz);
            if (!node.isObject()) {
                return reader.readValue(node);
            }

            final JsonNode keys = node.has(PAYLOAD_FIELD) ? node.get(PAYLOAD_FIELD) : node;
            final Iterator<String> keyFields = keys.fieldNames();
            if (keyFields.hasNext()) {
                final String id = keyFields.next();
                if (!keyFields.hasNext()) {
                    // Simple key
                    return reader.readValue(keys.get(id));
                }
                // Composite key
                return reader.readValue(keys);
            }
            return reader.readValue(keys);
        }
    }
}
