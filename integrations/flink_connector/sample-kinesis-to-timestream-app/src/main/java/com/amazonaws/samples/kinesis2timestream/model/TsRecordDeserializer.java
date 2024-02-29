package com.amazonaws.samples.kinesis2timestream.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class TsRecordDeserializer implements DeserializationSchema<SampleModel> {

    private final ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public SampleModel deserialize(byte[] messageBytes) {
//        objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
//        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
//        objectMapper.setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.ANY);
        try {
            return objectMapper.readValue(messageBytes, SampleModel.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize message", e);
        }
    }

    @Override
    public boolean isEndOfStream(SampleModel nextElement) {
        return false;
    }

    @Override
    public TypeInformation<SampleModel> getProducedType() {
        return TypeInformation.of(SampleModel.class);
    }
}
