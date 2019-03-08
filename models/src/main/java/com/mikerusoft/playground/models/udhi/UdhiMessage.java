package com.mikerusoft.playground.models.udhi;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(builderClassName = "Builder", toBuilder = true)
public class UdhiMessage {
    private short id;
    private short size;
    private short ind;
    private int providerId;
    private String text;

    public boolean udhi() {
        return size > 1;
    }
}
