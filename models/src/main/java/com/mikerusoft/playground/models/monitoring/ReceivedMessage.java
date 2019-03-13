package com.mikerusoft.playground.models.monitoring;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(builderClassName = "Builder", toBuilder = true)
public class ReceivedMessage implements Message {
    private long receivedTime;
    private String id;
    private String from;
    private String to;
    private String text;
}
