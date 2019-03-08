package com.mikerusoft.playground.kafkastreamsinit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JSONSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {

    private static final String JSON_POJO_CLASS_PROPERTY_NAME = "JsonPOJOClass";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Class<T> tClass;
    private ObjectMapper mapper;

    // it's for use via properties class
    public JSONSerde() {
        mapper = new ObjectMapper();
    }

    public JSONSerde(Class<T> tClass) {
        mapper = new ObjectMapper();
        this.tClass = tClass;
    }

    public JSONSerde(Class<T> tClass, ObjectMapper mapper) {
        this.tClass = tClass;
        this.mapper = mapper;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        if(tClass == null) {
            tClass = (Class<T>) configs.get(JSON_POJO_CLASS_PROPERTY_NAME);
        }
    }


    @Override
    public T deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return mapper.readValue(data, tClass);
        } catch (final IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        if (data == null) {
            return null;
        }

        try {
            return mapper.writeValueAsBytes(data);
        } catch (final Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {}

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }
}
