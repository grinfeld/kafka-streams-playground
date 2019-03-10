package com.mikerusoft.playground.models.udhi;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(builderClassName = "Builder", toBuilder = true)
public class GroupMessage {
    private short size;
    @lombok.Builder.Default
    private Set<UdhiMessage> parts = new HashSet<>();
    @lombok.Builder.Default
    private long timeIngested = System.currentTimeMillis();

    public boolean ready() {
        return size == parts.size();
    }

    public void add(UdhiMessage m) {
        parts.add(m);
    }

    public ReadyMessage convert() {
        UdhiMessage lastMessage = parts.stream().min((m1,m2) -> (int)(m1.getSentTime() - m2.getSentTime())).orElse(null);
        if (lastMessage == null)
            throw new IllegalArgumentException("Never should happen!");
        String content = parts.stream().sorted(Comparator.comparingInt(UdhiMessage::getInd))
                .map(UdhiMessage::getText).collect(Collectors.joining());
        return ReadyMessage.builder().fullMessage(parts.size() == size).id(lastMessage.getId()).sentTime(lastMessage.getSentTime())
                .text(content)
                .providerId(lastMessage.getProviderId()).build();
    }

    public List<ReadyMessage> expand() {
        return parts.stream().sorted(Comparator.comparingInt(UdhiMessage::getInd)).map(this::udhiToSingleReadyMessage).collect(Collectors.toList());
    }

    private ReadyMessage udhiToSingleReadyMessage(UdhiMessage u) {
        return ReadyMessage.builder()
                .sentTime(u.getSentTime())
                .fullMessage(false)
                .id(u.getId())
                .text(u.getText())
                .providerId(u.getProviderId())
                .build();

    }
}
