package com.amazonaws.samples.kinesis2timestream.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValue;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.TimeUnit;

public class TimestreamRecordConverter {

    static Random rand = new Random();
    public static Record convert(final MyHostBase customObject) {
        if (customObject.getClass().equals(MyHostMetric.class)) {
            return convertFromMetric((MyHostMetric) customObject);
        } else if (customObject.getClass().equals(MyHostEvent.class)) {
            return convertFromEvent((MyHostEvent) customObject);
        } else {
            throw new RuntimeException("Invalid object type: " + customObject.getClass().getSimpleName());
        }
    }

    public static Record convertMultiRecords(final SampleModel sampleModel) {
        List<Dimension> dimensions = List.of(
                Dimension.builder()
                        .name("vin")
                        .value(sampleModel.getVin()).build());

        List<MeasureValue> measureValues = List.of(
                MeasureValue.builder()
                        .name("odometer")
                        .type(MeasureValueType.BIGINT)
                        .value(sampleModel.getOdometer()).build(),
                MeasureValue.builder()
                        .name("speed")
                        .type(MeasureValueType.VARCHAR)
                        .value(sampleModel.getSpeed()).build(),
                MeasureValue.builder()
                        .name("battery_state_of_health")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(sampleModel.getBattery_state_of_health())).build(),
                MeasureValue.builder()
                        .name("temperature")
                        .type(MeasureValueType.BIGINT)
                        .value(Integer.toString(sampleModel.getTemperature())).build()
        );

        return Record.builder()
                .dimensions(dimensions)
                .measureName("events_record")
                .measureValueType("MULTI")
                .measureValues(measureValues)
                .timeUnit(TimeUnit.MILLISECONDS)
                .time(Long.toString(sampleModel.getTimestamp())).build();
    }

    public static List<Record> convertRecords(final SampleModel sampleModel) {
        List<Dimension> dimensions = List.of(
                Dimension.builder()
                        .name("vin")
                        .value(sampleModel.getVin()).build());

        List<Record> records = new ArrayList<>();
            records.add(Record.builder()
                    .dimensions(dimensions)
                    .measureName("speed")
                    .measureValue(sampleModel.getSpeed())
                    .measureValueType(MeasureValueType.VARCHAR)
                    .timeUnit(TimeUnit.MILLISECONDS)
                    .time(Long.toString(sampleModel.getTimestamp())).build());

        records.add(Record.builder()
                .dimensions(dimensions)
                .measureName("battery_state_of_health")
                .measureValue(doubleToString(sampleModel.getBattery_state_of_health()))
                .measureValueType(MeasureValueType.DOUBLE)
                .timeUnit(TimeUnit.MILLISECONDS)
                .time(Long.toString(sampleModel.getTimestamp())).build());


        records.add(Record.builder()
                .dimensions(dimensions)
                .measureName("odometer")
                .measureValue(sampleModel.getOdometer())
                .measureValueType(MeasureValueType.VARCHAR)
                .timeUnit(TimeUnit.MILLISECONDS)
                .time(Long.toString(sampleModel.getTimestamp())).build());
        return records;
    }

    public static Record convertRecords2MultiRecord(final SampleDp sampleDp) {
        List<Dimension> dimensions = List.of(
                Dimension.builder()
                        .name("vin_next1")
                        .value(sampleDp.getDimensionValue()).build());

        List<MeasureValue> measureValues = new ArrayList<>();

        for (int i = 0; i < sampleDp.getMeasureName().size(); i++) {
            String measureName = sampleDp.getMeasureName().get(i);
            String measureValue = sampleDp.getMeasureValue().get(i);
            MeasureValue measureValueObj = MeasureValue.builder()
                    .name(measureName)
                    .type(MeasureValueType.VARCHAR)
                    .value((measureValue)).build();
            measureValues.add(measureValueObj);
        }


        return Record.builder()
                .dimensions(dimensions)
                .measureName("speed")
                .measureValueType("MULTI")
                .measureValues(measureValues)
                .timeUnit(TimeUnit.MILLISECONDS)
                .time(Long.toString(System.currentTimeMillis())).build();

    }


    private static Record convertFromEvent(final MyHostEvent event) {
        List<Dimension> dimensions = List.of(
                Dimension.builder()
                        .name("region")
                        .value(event.getRegion()).build(),
                Dimension.builder()
                        .name("cell")
                        .value(event.getCell()).build(),
                Dimension.builder()
                        .name("silo")
                        .value(event.getSilo()).build(),
                Dimension.builder()
                        .name("availability_zone")
                        .value(event.getAvailabilityZone()).build(),
                Dimension.builder()
                        .name("microservice_name")
                        .value(event.getMicroserviceName()).build(),
                Dimension.builder()
                        .name("instance_name")
                        .value(event.getInstanceName()).build(),
                Dimension.builder()
                        .name("process_name")
                        .value(event.getProcessName()).build(),
                Dimension.builder()
                        .name("jdk_version")
                        .value(event.getJdkVersion())
                        .build()
        );

        List<MeasureValue> measureValues = List.of(
                MeasureValue.builder()
                        .name("task_completed")
                        .type(MeasureValueType.BIGINT)
                        .value(Integer.toString(event.getTaskCompleted())).build(),
                MeasureValue.builder()
                        .name("task_end_state")
                        .type(MeasureValueType.VARCHAR)
                        .value(event.getTaskEndState()).build(),
                MeasureValue.builder()
                        .name("gc_reclaimed")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(event.getGcReclaimed())).build(),
                MeasureValue.builder()
                        .name("gc_pause")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(event.getGcPause())).build(),
                MeasureValue.builder()
                        .name("memory_free")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(event.getMemoryFree())).build()
        );

        return Record.builder()
                .dimensions(dimensions)
                .measureName("events_record")
                .measureValueType("MULTI")
                .measureValues(measureValues)
                .timeUnit(TimeUnit.SECONDS)
                .time(Long.toString(event.getTime())).build();
    }

    private static Record convertFromMetric(final MyHostMetric metric) {
        List<Dimension> dimensions = List.of(
                Dimension.builder()
                        .name("region")
                        .value(metric.getRegion()).build(),
                Dimension.builder()
                        .name("cell")
                        .value(metric.getCell()).build(),
                Dimension.builder()
                        .name("silo")
                        .value(metric.getSilo()).build(),
                Dimension.builder()
                        .name("availability_zone")
                        .value(metric.getAvailabilityZone()).build(),
                Dimension.builder()
                        .name("microservice_name")
                        .value(metric.getMicroserviceName()).build(),
                Dimension.builder()
                        .name("instance_type")
                        .value(metric.getInstanceType()).build(),
                Dimension.builder()
                        .name("os_version")
                        .value(metric.getOsVersion()).build(),
                Dimension.builder()
                        .name("instance_name")
                        .value(metric.getInstanceName()).build()
        );

        List<MeasureValue> measureValues = List.of(
                MeasureValue.builder()
                        .name("cpu_user")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getCpuUser())).build(),
                MeasureValue.builder()
                        .name("cpu_system")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getCpuSystem())).build(),
                MeasureValue.builder()
                        .name("cpu_steal")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getCpuSteal())).build(),
                MeasureValue.builder()
                        .name("cpu_iowait")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getCpuIowait())).build(),
                MeasureValue.builder()
                        .name("cpu_nice")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getCpuNice())).build(),
                MeasureValue.builder()
                        .name("cpu_hi")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getCpuHi())).build(),
                MeasureValue.builder()
                        .name("cpu_si")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getCpuSi())).build(),
                MeasureValue.builder()
                        .name("cpu_idle")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getCpuIdle())).build(),
                MeasureValue.builder()
                        .name("memory_free")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getMemoryFree())).build(),
                MeasureValue.builder()
                        .name("memory_used")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getMemoryUsed())).build(),
                MeasureValue.builder()
                        .name("memory_cached")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getMemoryCached())).build(),
                MeasureValue.builder()
                        .name("disk_io_reads")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getDiskIOReads())).build(),
                MeasureValue.builder()
                        .name("disk_io_writes")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getDiskIOWrites())).build(),
                MeasureValue.builder()
                        .name("latency_per_read")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getLatencyPerRead())).build(),
                MeasureValue.builder()
                        .name("latency_per_write")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getLatencyPerWrite())).build(),
                MeasureValue.builder()
                        .name("network_bytes_in")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getNetworkBytesIn())).build(),
                MeasureValue.builder()
                        .name("network_bytes_out")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getNetworkBytesOut())).build(),
                MeasureValue.builder()
                        .name("disk_used")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getDiskUsed())).build(),
                MeasureValue.builder()
                        .name("disk_free")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getDiskFree())).build(),
                MeasureValue.builder()
                        .name("file_descriptors_in_use")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getFileDescriptorInUse())).build()
        );


        return Record.builder()
                .dimensions(dimensions)
                .measureName("metrics_record")
                .measureValueType("MULTI")
                .measureValues(measureValues)
                .timeUnit(TimeUnit.SECONDS)
                .time(Long.toString(metric.getTime())).build();
    }

    private static String doubleToString(double inputDouble) {
        // Avoid sending -0.0 (negative double) to Timestream - it throws ValidationException
        if (Double.valueOf(-0.0).equals(inputDouble)) {
            return "0.0";
        }
        return Double.toString(inputDouble);
    }

}
