package com.mikerusoft.playground.models.simple;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class Counter {
    @Getter
    private long counter;

    public Counter op(long value) {
        counter = counter + value;
        return this;
    }
}
