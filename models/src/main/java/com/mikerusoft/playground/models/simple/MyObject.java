package com.mikerusoft.playground.models.simple;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(builderClassName = "Builder", toBuilder = true)
public class MyObject {
    private long timestamp;
    private String value;
    private String key;

    public MyObject mergeWith(MyObject other) {
        Builder builder = MyObject.builder();
        if (other == null)
            return this.toBuilder().build();

        builder.value(this.value + "," + other.value);
        builder.key(this.key + "," + other.key);
        builder.timestamp(System.currentTimeMillis());

        return builder.build();
    }
}
