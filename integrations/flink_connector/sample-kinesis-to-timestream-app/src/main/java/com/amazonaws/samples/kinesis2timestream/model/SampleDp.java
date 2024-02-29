package com.amazonaws.samples.kinesis2timestream.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
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
public class SampleDp {

        @JsonProperty(value = "measure_name", required = true)
        private String measureName;

        @JsonProperty(value = "measureValue", required = true)
        private String measureValue;

        @JsonProperty(value = "dimensionName", required = true)
        private String dimensionName;

        @JsonProperty(value = "dimesnsionValue", required = true)
        private String dimensionValue;

}
