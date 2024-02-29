package com.amazonaws.samples.kinesis2timestream.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@Builder
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class SampleModel {
    @JsonProperty(value = "timestamp", required = true)
    private Long timestamp;

    @JsonProperty(value = "vin", required = true)
    private String vin;

    @JsonProperty(value = "odometer", required = true)
    private String odometer;

    @JsonProperty(value = "speed", required = true)
    private String speed;

    @JsonProperty(value = "battery_state_of_health", required = true)
    private Double battery_state_of_health;

    @JsonProperty(value = "temperature", required = true)
    private Integer temperature;
}
