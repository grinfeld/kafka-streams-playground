package com.mikerusoft.playground.models.simple;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(builderClassName = "Builder", toBuilder = true)
public class ErrorData {
    @JsonProperty("MIMEMESSAGEID")
    private String mimeMessageId;
    @JsonProperty("STATE")
    private String state;
    @JsonProperty("ERROR_MESSAGE")
    private String errorMessage;
    @JsonProperty("RECIPIENTS")
    private String recipients;
    @JsonProperty("FROM")
    private String from;
}
