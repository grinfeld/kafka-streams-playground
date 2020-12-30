package com.mikerusoft.cdc.serdes.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

public class JsonSerde<T> implements Serde<T> {
    private final ObjectMapper mapper;
    private final Class<T> clazz;
    private final TypeReference<T> typeRef;

    public JsonSerde(ObjectMapper mapper, Class<T> clazz) {
        this.mapper = mapper;
        this.clazz = clazz;
        this.typeRef = null;
    }

    public JsonSerde(ObjectMapper mapper, TypeReference<T> typeRef) {
        this.mapper = mapper;
        this.typeRef = typeRef;
        this.clazz = null;
    }

    @Override
    public Serializer<T> serializer() {
        return new JsonSerializer<>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new JsonDeserializer<>();
    }

    private class JsonSerializer<V> implements Serializer<V> {
        @Override
        public byte[] serialize(String s, V v) {
            try {
                return mapper.writeValueAsBytes(v);
            } catch (JsonProcessingException e) {
                return Utils.rethrowRuntime(e);
            }
        }
    }

    private class JsonDeserializer<V> implements Deserializer<T> {

        @Override
        public T deserialize(String s, byte[] bytes) {
            try {
                return typeRef == null ? mapper.readValue(bytes, clazz) : mapper.readValue(bytes, typeRef);
            } catch (IOException e) {
                return Utils.rethrowRuntime(e);
            }
        }
    }
}
