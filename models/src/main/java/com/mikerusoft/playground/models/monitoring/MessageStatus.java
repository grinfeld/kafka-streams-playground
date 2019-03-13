package com.mikerusoft.playground.models.monitoring;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(builderClassName = "Builder", toBuilder = true)
public class MessageStatus implements Message {
    private String id;
    private String providerId;
    private String from;
    private String to;
    private String extMessageId;
    private String status;
    private long statusTime;
}
