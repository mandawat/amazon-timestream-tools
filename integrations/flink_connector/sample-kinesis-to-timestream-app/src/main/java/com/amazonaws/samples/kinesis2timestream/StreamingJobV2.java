package com.amazonaws.samples.kinesis2timestream;

import com.amazonaws.samples.connectors.timestream.TimestreamSink;
import com.amazonaws.samples.connectors.timestream.TimestreamSinkConfig;
import com.amazonaws.samples.kinesis2timestream.kinesis.RoundRobinKinesisShardAssigner;
import com.amazonaws.samples.kinesis2timestream.model.SampleDp;
import com.amazonaws.samples.kinesis2timestream.model.SampleModel;
import com.amazonaws.samples.kinesis2timestream.model.TimestreamRecordConverter;
import com.amazonaws.samples.kinesis2timestream.model.TsRecordDeserializer;
import com.amazonaws.samples.kinesis2timestream.utils.ParameterToolUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class StreamingJobV2 {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    // Currently Timestream supports max. 100 records in single write request. Do not increase this value.
    private static final int MAX_TIMESTREAM_RECORDS_IN_WRITERECORDREQUEST = 100;
    private static final int MAX_CONCURRENT_WRITES_TO_TIMESTREAM = 1000;

    public static final String DEFAULT_STREAM_NAME = "TimestreamTestStream";
    public static final String DEFAULT_REGION_NAME = "us-east-1";

    public static DataStream<SampleModel> createKinesisSource(StreamExecutionEnvironment env, ParameterTool parameter) throws Exception {

        //set Kinesis consumer properties
        Properties kinesisConsumerConfig = new Properties();
        //set the region the Kinesis stream is located in
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION,
                parameter.get("Region", DEFAULT_REGION_NAME));
        //obtain credentials through the DefaultCredentialsProviderChain, which includes the instance metadata
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");

        String adaptiveReadSettingStr = parameter.get("SHARD_USE_ADAPTIVE_READS", "false");

        if(adaptiveReadSettingStr.equals("true")) {
            kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS, "true");
        } else {
            //poll new events from the Kinesis stream once every second
            kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
                    parameter.get("SHARD_GETRECORDS_INTERVAL_MILLIS", "1000"));
            // max records to get in shot
            kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
                    parameter.get("SHARD_GETRECORDS_MAX", "10000"));
        }

        //create Kinesis source
        FlinkKinesisConsumer<SampleModel> flinkKinesisConsumer = new FlinkKinesisConsumer<>(
                //read events from the Kinesis stream passed in as a parameter
                parameter.get("InputStreamName", DEFAULT_STREAM_NAME),
                //deserialize events with EventSchema
                new TsRecordDeserializer(),
                //using the previously defined properties
                kinesisConsumerConfig
        );
        flinkKinesisConsumer.setShardAssigner(new RoundRobinKinesisShardAssigner());

        return env
                .addSource(flinkKinesisConsumer)
                .name("KinesisSource");
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SampleModel> mappedInput = createKinesisSource(env, parameter);

        System.out.println("Mapped Input : " + mappedInput);

        String region = parameter.get("Region", "us-east-1");
        String databaseName = parameter.get("TimestreamDbName", "kdaflink");
        String tableName = parameter.get("TimestreamTableName", "kinesisdata");
        long memoryStoreTTLHours = Long.parseLong(parameter.get("MemoryStoreTTLHours", "24"));
        long magneticStoreTTLDays = Long.parseLong(parameter.get("MagneticStoreTTLDays", "7"));

        // EndpointOverride is optional. Learn more here: https://docs.aws.amazon.com/timestream/latest/developerguide/architecture.html#cells
        String endpointOverride = parameter.get("EndpointOverride", "");
        if (endpointOverride.isEmpty()) {
            endpointOverride = null;
        }

        TimestreamInitializer timestreamInitializer = new TimestreamInitializer(region, endpointOverride);
        timestreamInitializer.createDatabase(databaseName);
        timestreamInitializer.createTable(databaseName, tableName, memoryStoreTTLHours, magneticStoreTTLDays);

        TimestreamSink<SampleDp> sink = new TimestreamSink<>(
                (recordObject, context) -> TimestreamRecordConverter.convertRecords2(recordObject),
                (List<Record> records) -> {
                    LOG.debug("Preparing WriteRecordsRequest with {} records", records.size());
                    return WriteRecordsRequest.builder()
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .records(records)
                            .build();
                },
                TimestreamSinkConfig.builder()
                        .maxBatchSize(MAX_TIMESTREAM_RECORDS_IN_WRITERECORDREQUEST)
                        .maxBufferedRequests(100 * MAX_TIMESTREAM_RECORDS_IN_WRITERECORDREQUEST)
                        .maxInFlightRequests(MAX_CONCURRENT_WRITES_TO_TIMESTREAM)
                        .maxTimeInBufferMS(15000)
                        .emitSinkMetricsToCloudWatch(true)
                        .writeClientConfig(TimestreamSinkConfig.WriteClientConfig.builder()
                                .maxConcurrency(MAX_CONCURRENT_WRITES_TO_TIMESTREAM)
                                .maxErrorRetry(10)
                                .region(region)
                                .requestTimeout(Duration.ofSeconds(20))
                                .endpointOverride(endpointOverride)
                                .build())
                        .failureHandlerConfig(TimestreamSinkConfig.FailureHandlerConfig.builder()
                                .failProcessingOnErrorDefault(true)
                                .failProcessingOnRejectedRecordsException(true)
                                .printFailedRequests(false)
                                .build())
                        .build()
        );

//        Kryo.reset
        mappedInput.flatMap(new FlatMapFunction<SampleModel, SampleDp>() {
            @Override
            public void flatMap(SampleModel sampleModel, Collector<SampleDp> collector) throws Exception {
                SampleDp sampleDp = SampleDp.builder().measureName("speed")
                        .measureValue(sampleModel.getSpeed())
                        .dimensionName("vin")
                        .dimensionValue(sampleModel.getVin()).build();
                collector.collect(sampleDp);

                SampleDp sampleDp2 = SampleDp.builder().measureName("odometer")
                        .measureValue(sampleModel.getOdometer())
                        .dimensionName("vin")
                        .dimensionValue(sampleModel.getVin()).build();
                collector.collect(sampleDp2);
            }
        }).sinkTo(sink)
        .disableChaining();

//        mappedInput.flatMap((SampleModel sampleModel, Collector<Record> out, Record) -> {
//            List<Record> records = TimestreamRecordConverter.convertRecords(sampleModel);
//           System.out.println("Records found: " + records.size());
//            records.stream().forEach(record -> {
//                out.collect(record);
//            });
//        }).sinkTo(sink)
//                .disableChaining();

        env.execute("Flink Streaming Java API Skeleton");
    }


}
