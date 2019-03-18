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
    @lombok.Builder.Default
    private long currentTime = System.currentTimeMillis();

    private int counter;

    public MessageMonitor increaseCounter(int i) {
        counter = counter + i;
        return this;
    }

    public MessageMonitor merge(MessageMonitor mergeWith) {
        Builder builder = this.toBuilder();
        if (mergeWith == null)
            return builder.build();
        if (mergeWith.getId() != null)
            builder.id(mergeWith.getId());
        if (mergeWith.getExtMessageId() != null)
            builder.extMessageId(mergeWith.getExtMessageId());
        if (mergeWith.getProviderId() != null)
            builder.providerId(mergeWith.getProviderId());
        if (mergeWith.getDrStatus() != null)
            builder.drStatus(mergeWith.getDrStatus());
        if (mergeWith.getDrStatusTime() > 0)
            builder.drStatusTime(mergeWith.getDrStatusTime());
        if (mergeWith.getSentStatus() != null)
            builder.sentStatus(mergeWith.getSentStatus());
        if (mergeWith.getSentStatusTime() > 0)
            builder.sentStatusTime(mergeWith.getSentStatusTime());
        if (mergeWith.getSentTime() > 0)
            builder.sentTime(mergeWith.getSentTime());
        if (mergeWith.getReceivedTime() > 0)
            builder.receivedTime(mergeWith.getReceivedTime());
        MessageMonitor messageMonitor = builder.build().increaseCounter(mergeWith.counter);

        return messageMonitor;
    }
}
