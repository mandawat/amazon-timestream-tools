package com.amazonaws.samples.kinesis2timestream;

import com.amazonaws.samples.kinesis2timestream.model.SampleModel;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.amazonaws.samples.kinesis2timestream.StreamingJobV2.DEFAULT_REGION_NAME;
import static com.amazonaws.samples.kinesis2timestream.StreamingJobV2.DEFAULT_STREAM_NAME;

public class KenesisDataProducer {

    public static void main(String[] args) throws IOException {
        Properties kinesisConsumerConfig = new Properties();
        //set the region the Kinesis stream is located in
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION, DEFAULT_REGION_NAME);
        //obtain credentials through the DefaultCredentialsProviderChain, which includes the instance metadata
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");


        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();

        clientBuilder.setRegion(DEFAULT_REGION_NAME);
//        clientBuilder.setCredentials(credentialsProvider);
//        clientBuilder.setClientConfiguration(config);

        AmazonKinesis kinesisClient = clientBuilder.build();

//        String json = "{/""time/": “1709133423746” ,“vin”: “ABC1234”, “odometer”: 10, “speed”: 200, “battery_state_of_health”: “75.2”, “temperature”: 104}";
        ObjectMapper objectMapper = new ObjectMapper();
//        objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
//        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
//        objectMapper.setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.ANY);

        PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
        putRecordsRequest.setStreamName(DEFAULT_STREAM_NAME);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
        SampleModel sampleModel = SampleModel.builder()
                .timestamp(System.currentTimeMillis() + i)
                .vin("ABC1234")
                .odometer("10L" + i)
                .speed("200" + i)
                .battery_state_of_health(75.2)
                .temperature(104)
                .build();
            PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
            byte [] messageBytes = objectMapper.writeValueAsBytes(sampleModel);
            putRecordsRequestEntry.setData(ByteBuffer.wrap(messageBytes));
            putRecordsRequestEntry.setPartitionKey(String.format("partitionKeyNext-%d", i));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);

            SampleModel newSampleModel =  objectMapper.readValue(messageBytes, SampleModel.class);
            System.out.println(newSampleModel);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        PutRecordsResult putRecordsResult  = kinesisClient.putRecords(putRecordsRequest);
        System.out.println("Put Result" + putRecordsResult);



    }
}
