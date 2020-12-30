package com.mikerusoft.cdc.serdes.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.function.Function;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MutablePair<K, V> {
    private K key;
    private V value;

    public boolean isEmpty() {
        return key == null;
    }

    public <VR> MutablePair<K, VR> mapValue(Function<V, VR> func) {
        return new MutablePair<>(this.key, func.apply(this.value));
    }

    public MutablePair<K, V> updateValueWith(Function<V, V> func) {
        this.value = func.apply(this.value);
        return this;
    }

    public MutablePair<K, V> update(K key, V value) {
        this.key = key;
        this.value = value;
        return this;
    }
}
