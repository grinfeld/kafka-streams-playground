package com.mikerusoft.cdc.serdes.utils;

import com.mikerusoft.cdc.serdes.model.SectionKey;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class KeySerializer implements Serializer<SectionKey> {
    @Override
    public byte[] serialize(String s, SectionKey section) {
        return Utils.generateKey(section).getBytes(StandardCharsets.UTF_8);
    }
}
