package com.mikerusoft.playground.generatedata.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;

public class ProducerCreator {

    public static <K, V> KafkaProducer<K, V> createProducer(String url, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);

        return new KafkaProducer<>(props, keySerializer, valueSerializer);
    }
}
