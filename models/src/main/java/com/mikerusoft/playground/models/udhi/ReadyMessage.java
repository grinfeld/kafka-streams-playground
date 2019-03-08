package com.mikerusoft.playground.models.udhi;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(builderClassName = "Builder", toBuilder = true)
public class ReadyMessage {
    private short id;
    private int providerId;
    private String text;
    private boolean fullMessage;
}