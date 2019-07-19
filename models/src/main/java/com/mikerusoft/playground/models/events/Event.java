package com.mikerusoft.playground.models.events;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(builderClassName = "Builder", toBuilder = true)
public class Event {
    private String id; // sectionId
    private long timestamp;
    private String data;
}
