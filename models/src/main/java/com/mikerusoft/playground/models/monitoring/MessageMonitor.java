package com.mikerusoft.playground.models.monitoring;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(builderClassName = "Builder", toBuilder = true)
public class MessageMonitor {
    private String id;
    private String providerId;
    private long sentTime;
    private long receivedTime;
    private long drStatusTime;
    private String drStatus;
    private String extMessageId;
    private long sentStatusTime;
    private String sentStatus;
}
