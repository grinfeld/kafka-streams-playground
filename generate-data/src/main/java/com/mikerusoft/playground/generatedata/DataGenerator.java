package com.mikerusoft.playground.generatedata;

import reactor.core.publisher.Flux;

import java.util.function.Function;
import java.util.stream.IntStream;

public class DataGenerator {

    public static IntStream generateSequence(Function<Integer, Integer> nextInt) {
        return IntStream.iterate(0, nextInt::apply);
    }

    public static Flux<Integer> generateFlux(Function<Integer, Integer> nextInt) {
        return Flux.fromStream(() -> generateSequence(nextInt).boxed());
    }
}
