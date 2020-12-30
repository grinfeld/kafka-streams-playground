package com.mikerusoft.cdc.serdes.utils;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class KStreamWrapper<T> {
    @Getter private final KStream<String, T> stream;

    public static <T> KStreamWrapper<T> of(KStream<String, T> stream) {
        return new KStreamWrapper<>(stream.filter((key, value) -> value != null));
    }

    public KStreamWrapper<T> filterByValue(java.util.function.Predicate<T> predicate) {
        return KStreamWrapper.of(stream.filter((key, value) -> predicate.test(value)));
    }

    public <R> KStreamWrapper<R> mapByValueAndFilterNulls(Function<T, R> mapFunc) {
        return KStreamWrapper.of(stream.mapValues((key, value) -> mapFunc.apply(value)));
    }

    public <R> KStreamWrapper<R> mapByValue(Function<T, R> mapFunc) {
        return new KStreamWrapper<>(stream.mapValues((key, value) -> mapFunc.apply(value)));
    }

    public <R> KStreamWrapper<R> flatMapByValueAndFilterNulls(Function<T, Iterable<R>> mapFunc) {
        return KStreamWrapper.of(stream.flatMapValues((key, value) -> mapFunc.apply(value)));
    }

    public KStreamWrapper<T> peekValue(Consumer<T> valueConsumer) {
        return KStreamWrapper.of(stream.peek((key, value) -> valueConsumer.accept(value)));
    }

    public <R, SK, SV> KStreamWrapper<R> transformValuesWithStore(BiFunction<T, KeyValueStore<SK, SV>, R> func, String stateStore) {
        return KStreamWrapper.of(this.stream.transformValues(() -> new ValueTransformerWithStateStore<>(func, stateStore), stateStore));
    }

    public <R, SK, SV> KStreamWrapper<R> transformValuesWithGlobalStore(BiFunction<T, KeyValueStore<SK, SV>, R> func, String stateStore) {
        return KStreamWrapper.of(this.stream.transformValues(() -> new ValueTransformerWithStateStore<>(func, stateStore)));
    }

    private static class ValueTransformerWithStateStore<T, R, SK, SV> implements ValueTransformer<T, R> {

        private final BiFunction<T, KeyValueStore<SK, SV>, R> func;
        private final String stateStore;

        public ValueTransformerWithStateStore(BiFunction<T, KeyValueStore<SK, SV>, R> func, String stateStore) {
            this.func = func;
            this.stateStore = stateStore;
        }

        private KeyValueStore<SK, SV> store;

        @Override
        public void init(ProcessorContext context) {
            store = context.getStateStore(stateStore);
        }

        @Override
        public R transform(T value) {
            return func.apply(value, store);
        }

        @Override
        public void close() {}
    }
}
