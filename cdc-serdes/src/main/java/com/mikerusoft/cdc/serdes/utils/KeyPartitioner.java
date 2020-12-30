package com.mikerusoft.cdc.serdes.utils;

import com.mikerusoft.cdc.serdes.model.SectionKey;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class KeyPartitioner implements Partitioner {

    private static final Partitioner def = new DefaultPartitioner();
    private static final Partitioner rb = new RoundRobinPartitioner();

    @Override
    public int partition(String s, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (key == null) return rb.partition(s, key, keyBytes, value, valueBytes, cluster);
        if (!(key instanceof SectionKey)) return rb.partition(s, key, keyBytes, value, valueBytes, cluster);
        SectionKey skey = (SectionKey) key;
        String newKey = skey.getSectionId() + "|" + skey.getDyid();
        return def.partition(s, newKey, newKey.getBytes(StandardCharsets.UTF_8), value, valueBytes, cluster);
    }

    @Override
    public void close() {
        // def.close(); ????
        // rb.close(); ????
        // do nothing
    }

    @Override
    public void configure(Map<String, ?> map) {
        // do nothing
    }
}
