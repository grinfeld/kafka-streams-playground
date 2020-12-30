package com.mikerusoft.cdc.serdes.utils;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.*;
import java.util.stream.Stream;

public interface Pair<K, V> extends Map.Entry<K, V> {

    static <K, V> Pair<K, V> of(K key, V value) {
        return new ImmutableMap<>(key, value);
    }

    static <K, V> Pair<K, V> of(Map.Entry<K, V> entry) {
        return new ImmutableMap<>(entry.getKey(), entry.getValue());
    }

    <KT, VT> Pair<KT, VT> map(BiFunction<K, V, Pair<KT, VT>> biFunc);
    <VT> Pair<K, VT> mapValue(Function<V, VT> func);
    boolean isValue(Predicate<V> predicate);
    boolean isKey(Predicate<K> predicate);
    Pair<K, V> filter(Predicate<Pair<K, V>> predicate);
    Pair<K, V> filterByValue(Predicate<V> predicate);
    Pair<K, V> filterByKey(Predicate<K> predicate);
    boolean isEmpty() ;
    boolean isPresent() ;
    boolean isKey(K key);
    boolean isNotKey(K key);
    Pair<K, V> filterNotByKey(Predicate<K> predicate);

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @ToString
    class ImmutableMap<K, V> implements Pair<K, V> {
        private final K key;
        private final V value;

        public K getKey() { return key; }
        public V getValue() { return value; }
        public V setValue(V value) { throw new UnsupportedOperationException(); }

        public <KT, VT> Pair<KT, VT> map(BiFunction<K, V, Pair<KT, VT>> biFunc) {
            return biFunc.apply(key, value);
        }

        public <VT> Pair<K, VT> mapValue(Function<V, VT> func) {
            return Pair.of(key, func.apply(value));
        }

        public Pair<K, V> filter(Predicate<Pair<K, V>> predicate) {
            return predicate.test(this) ? this : EmptyPair.empty();
        }

        public Pair<K, V> filterByValue(Predicate<V> predicate) {
            return predicate.test(getValue()) ? this : EmptyPair.empty();
        }

        public Pair<K, V> filterByKey(Predicate<K> predicate) {
            return predicate.test(getKey()) ? this : EmptyPair.empty();
        }

        public Pair<K, V> filterNotByKey(Predicate<K> predicate) {
            return !predicate.test(getKey()) ? this : EmptyPair.empty();
        }

        public boolean isValue(Predicate<V> predicate) {
            return predicate.test(value);
        }

        public boolean isKey(Predicate<K> predicate) {
            return predicate.test(key);
        }

        public boolean isKey(K key) {
            return Objects.equals(key, this.key);
        }

        public boolean isNotKey(K key) {
            return !Objects.equals(key, this.key);
        }

        public boolean isEmpty() { return false; }
        public boolean isPresent() { return true; }

        public Pair<V, K> reverse() {
            return Pair.of(this.getValue(), this.getKey());
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?,?> e = (Map.Entry<?,?>)o;
            return Objects.equals(key, e.getKey()) && Objects.equals(value, e.getValue());
        }

        @Override
        public int hashCode() {
            return (key   == null ? 0 :   key.hashCode()) ^ (value == null ? 0 : value.hashCode());
        }
    }

    static <K,V> Pair<K,V> empty() {
        return EmptyPair.empty();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    class EmptyPair<K, V> implements Pair<K, V> {

        private final static Pair<?, ?> empty = new EmptyPair<>();
        private static <K, V> Pair<K, V> empty() { return new EmptyPair<>(); }

        public K getKey() { throw new IllegalArgumentException(); }
        public V getValue() { throw new IllegalArgumentException(); }
        public V setValue(V v) { throw new IllegalArgumentException(); }

        public <KT, VT> Pair<KT, VT> map(BiFunction<K, V, Pair<KT, VT>> biFunc) { return empty(); }
        public <VT> Pair<K, VT> mapValue(Function<V, VT> func) { return empty(); }
        public boolean isValue(Predicate<V> predicate) { return false; }
        public boolean isKey(Predicate<K> predicate) { return false; }
        public Pair<K, V> filter(Predicate<Pair<K, V>> predicate) { return empty(); }
        public Pair<K, V> filterByValue(Predicate<V> predicate) { return empty(); }
        public Pair<K, V> filterByKey(Predicate<K> predicate) { return empty(); }
        public boolean isEmpty() { return true; }
        public boolean isPresent() { return false; }
        public boolean isKey(K key) { return false; }
        public boolean isNotKey(K key) { return false; }
        public Pair<K, V> filterNotByKey(Predicate<K> predicate) { return empty(); }

        @Override public int hashCode() { return empty.hashCode(); }
        @Override public boolean equals(Object obj) {
            return obj.getClass() ==getClass() && empty.equals(obj);
        }

        public Map<K, V> toMap() {
            Map<K, V> map = new HashMap<>();
            map.put(this.getKey(), this.getValue());
            return map;
        }
    }

    class PStream<K, V> {
        private final Stream<Pair<K, V>> stream;

        public static <K, V> PStream<K, V> of(Stream<Pair<K, V>> stream) {
            return new PStream<>(stream);
        }

        public static <K, V> PStream<K, V> ofEntries(Stream<Map.Entry<K, V>> stream) {
            return new PStream<>(stream.map(Pair::of));
        }

        private PStream(Stream<Pair<K, V>> stream) {
            Objects.requireNonNull(stream);
            this.stream = stream;
        }

        public Stream<Pair<K, V>> toStream() {
            return stream;
        }

        public <KT, VT> PStream<KT, VT> map(BiFunction<K, V, Pair<KT, VT>> mapFunc) {
            return PStream.of(stream.map(p -> mapFunc.apply(p.getKey(), p.getValue())));
        }

        public <KT, VT> PStream<KT, VT> map(Function<Pair<K, V>, Pair<KT, VT>> mapFunc) {
            return PStream.of(stream.map(mapFunc));
        }

        public <VT> PStream<K, VT> mapValue(Function<V, VT> mapValueFunc) {
            return PStream.of(stream.map(pair -> pair.mapValue(mapValueFunc)));
        }

        public PStream<K, V> filter(BiPredicate<K, V> predicate) {
            return PStream.of(stream.filter(pair -> predicate.test(pair.getKey(), pair.getValue())));
        }

        public PStream<K, V> filterPair(Predicate<Pair<K, V>> predicate) {
            return PStream.of(stream.filter(predicate));
        }

        public PStream<K, V> filterNotPair(Predicate<Pair<K, V>> predicate) {
            return PStream.of(stream.filter(pair -> !predicate.test(pair)));
        }

        public PStream<K, V> filterByKey(Predicate<K> predicate) {
            return PStream.of(stream.filter(pair -> predicate.test(pair.getKey())));
        }

        public PStream<K, V> filterNotByKey(Predicate<K> predicate) {
            return PStream.of(stream.filter(pair -> !predicate.test(pair.getKey())));
        }

        public PStream<K, V> filterByValue(Predicate<V> predicate) {
            return PStream.of(stream.filter(pair -> predicate.test(pair.getValue())));
        }

        public PStream<K, V> filterNotByValue(Predicate<V> predicate) {
            return PStream.of(stream.filter(pair -> !predicate.test(pair.getValue())));
        }

        public PStream<K, V> filter(Predicate<Pair<K, V>> predicate) {
            return PStream.of(stream.filter(predicate));
        }

        /**
         * Collects stream of pairs into Map. The difference between regular Stream.collect(Collectors.toMap(...))
         * is that regular doesn't support null values
         * @return Map built this stream of Pairs
         */
        public Map<K, V> collectToMap() {
            return stream.collect(
                    HashMap::new,
                    (map, pair) -> map.put(pair.getKey(), pair.getValue()),
                    (BiConsumer<Map<K, V>, Map<K, V>>) (map, map2) -> map.putAll(map2 == null ? new HashMap<>(0) : map2)
            );
        }
    }
}
