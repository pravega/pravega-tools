/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli.commands.utils;

import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.tools.pravegacli.commands.troubleshoot.Fault;
import io.pravega.tools.pravegacli.commands.troubleshoot.Record;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.pravega.shared.segment.StreamSegmentNameUtils.getSegmentNumber;

/**
 * Class for methods to output various metadata records.
 */
public class OutputUtils {

    /**
     * Method to output the fault map
     *
     * @param faults A mapping between the records and their list of faults
     * @return The information in the form of a String.
     */
    public static String outputFaults(Map<Record, Set<Fault>> faults) {
        StringBuilder responseBuilder = new StringBuilder();
        AtomicInteger serialNumber = new AtomicInteger(1);

        faults.forEach((k, v) -> {
            responseBuilder.append(serialNumber.get()).append(") ");
            responseBuilder.append(k.toString())
                    .append("-----------------------").append("\n");

            v.forEach(f -> {
                responseBuilder.append(f.getInconsistencyType()).append("\n");

                if (f.getInconsistentWith() != null) {
                    responseBuilder.append(f.getInconsistentWith().toString()).append("\n");
                }

                responseBuilder.append(f.getErrorMessage()).append("\n\n\n");
            });

            int length = responseBuilder.length();
            responseBuilder.delete(length-3, length-1);

            responseBuilder.append("-----------------------").append("\n\n");
            serialNumber.addAndGet(1);
        });

        int length = responseBuilder.length();
        if (length > 0) {
            responseBuilder.delete(length - 2, length - 1);
        }

        return responseBuilder.toString();
    }

    /**
     * Method to output an EpochTransitionRecord.
     *
     * @param record  EpochTransitionRecord
     * @return The record in the form a string.
     */
    public static String outputTransition(EpochTransitionRecord record) {
        StringBuilder responseBuilder = new StringBuilder();

        if (record == null) {
            responseBuilder.append("Record is null").append("\n");
            return responseBuilder.toString();
        }

        responseBuilder.append("The active epoch: ").append(tryOutputValue(record, EpochTransitionRecord::getActiveEpoch))
                .append(", creation time: ").append(tryOutputValue(record, EpochTransitionRecord::getTime)).append("\n");

        try {
            List<Integer> segmentsToSeal = record.getSegmentsToSeal().stream().map(StreamSegmentNameUtils::getSegmentNumber).collect(Collectors.toList());
            responseBuilder.append("Segments to seal: ").append(segmentsToSeal).append("\n");
        } catch (Exception e) {
            responseBuilder.append("\n");
        }

        responseBuilder.append("New Ranges: ").append("\n");
        try {
            record.getNewSegmentsWithRange().forEach(
                    (id, range) -> {
                        responseBuilder.append(getSegmentNumber(id)).append(" -> ");
                        responseBuilder.append("(").append(range.getKey())
                                .append(", ").append(range.getValue()).append(")").append("\n");
                    });
        } catch (Exception e) {
            responseBuilder.append("\n");
        }

        return responseBuilder.toString();
    }

    /**
     * Method to output an EpochRecord.
     *
     * @param record  EpochRecord
     * @return The record in the form a string.
     */
    public static String outputEpoch(EpochRecord record) {
        StringBuilder responseBuilder = new StringBuilder();

        if (record == null) {
            responseBuilder.append("Record is null").append("\n");
            return responseBuilder.toString();
        }

        responseBuilder.append("Stream epoch: ").append(tryOutputValue(record, EpochRecord::getEpoch)).append(", creation time: ")
                .append(tryOutputValue(record, EpochRecord::getCreationTime)).append("\n");
        responseBuilder.append("Segments in the epoch: ").append("\n");

        try {
            record.getSegments().forEach(segment -> responseBuilder.append("> ").append(segment.toString()).append("\n"));
        } catch (Exception e) {
            responseBuilder.append("\n");
        }

        return responseBuilder.toString();
    }

    /**
     * Method to output a HistoryTimeSeriesRecord.
     *
     * @param record  HistoryTimeSeriesRecord
     * @return The record in the form a string.
     */
    public static String outputHistoryRecord(HistoryTimeSeriesRecord record) {
        StringBuilder responseBuilder = new StringBuilder();

        if (record == null) {
            responseBuilder.append("Record is null").append("\n");
            return responseBuilder.toString();
        }

        responseBuilder.append("Stream epoch: ").append(tryOutputValue(record, HistoryTimeSeriesRecord::getEpoch)).append(", creation time: ")
                .append(tryOutputValue(record, HistoryTimeSeriesRecord::getScaleTime)).append("\n");

        responseBuilder.append("Segments created: ").append("\n");
        try {
            record.getSegmentsCreated().forEach(segment -> responseBuilder.append("> ").append(segment.toString()).append("\n"));
        } catch (Exception e) {
            responseBuilder.append("\n");
        }

        responseBuilder.append("Segments sealed: ").append("\n");
        try {
            record.getSegmentsSealed().forEach(segment -> responseBuilder.append("> ").append(segment.toString()).append("\n"));
        } catch (Exception e) {
            responseBuilder.append("\n");
        }

        return responseBuilder.toString();
    }

    /**
     * Method to output a StreamTruncationRecord.
     *
     * @param record  StreamTruncationRecord
     * @return The record in the form a string.
     */
    public static String outputTruncation(StreamTruncationRecord record) {
        StringBuilder responseBuilder = new StringBuilder();

        if (record == null) {
            responseBuilder.append("Record is null").append("\n");
            return responseBuilder.toString();
        }

        responseBuilder.append("Stream Cut: ").append(tryOutputValue(record, StreamTruncationRecord::getStreamCut)).append("\n");
        responseBuilder.append("Span: ").append(tryOutputValue(record, StreamTruncationRecord::getSpan)).append("\n");
        responseBuilder.append("Deleted Segments: ").append(tryOutputValue(record, StreamTruncationRecord::getDeletedSegments)).append("\n");
        responseBuilder.append("Segments to delete: ").append(tryOutputValue(record, StreamTruncationRecord::getToDelete)).append("\n");
        responseBuilder.append("Size till stream cut: ").append(tryOutputValue(record, StreamTruncationRecord::getSizeTill)).append("\n");
        responseBuilder.append("Updating: ").append(tryOutputValue(record, StreamTruncationRecord::isUpdating)).append("\n");
        responseBuilder.append("Span epoch low: ").append(tryOutputValue(record, StreamTruncationRecord::getSpanEpochLow)).append("\n");
        responseBuilder.append("Span epoch high: ").append(tryOutputValue(record, StreamTruncationRecord::getSpanEpochHigh)).append("\n");

        return responseBuilder.toString();
    }

    /**
     * Method to output a StreamConfigurationRecord.
     *
     * @param record  StreamConfigurationRecord
     * @return The record in the form a string.
     */
    public static String outputConfiguration(StreamConfigurationRecord record) {
        StringBuilder responseBuilder = new StringBuilder();

        if (record == null) {
            responseBuilder.append("Record is null").append("\n");
            return responseBuilder.toString();
        }

        responseBuilder.append("Scope: ").append(tryOutputValue(record, StreamConfigurationRecord::getScope)).append(", stream: ")
                .append(tryOutputValue(record, StreamConfigurationRecord::getStreamName)).append("\n");
        responseBuilder.append("Updating: ").append(tryOutputValue(record, StreamConfigurationRecord::isUpdating)).append("\n");

        responseBuilder.append("StreamConfiguration: ").append("\n");
        try {
            StreamConfiguration config = record.getStreamConfiguration();

            try {
                responseBuilder.append(config.getScalingPolicy() == null ? "ScalingPolicy(null)" : config.getScalingPolicy()).append("\n");
            } catch (Exception e) {
                responseBuilder.append("\n");
            }

            try {
                responseBuilder.append(config.getRetentionPolicy() == null ? "RetentionPolicy(null)" : config.getRetentionPolicy()).append("\n");
            } catch (Exception e) {
                responseBuilder.append("\n");
            }

        } catch (Exception e) {
            responseBuilder.append("\n");
        }

        return responseBuilder.toString();
    }

    /**
     * Method to output a CommittingTransactionsRecord.
     *
     * @param record  CommittingTransactionsRecord
     * @return The record in the form a string.
     */
    public static String outputCommittingTransactions(CommittingTransactionsRecord record) {
        StringBuilder responseBuilder = new StringBuilder();

        if (record == null) {
            responseBuilder.append("Record is null").append("\n");
            return responseBuilder.toString();
        }

        responseBuilder.append("Epoch: ").append(tryOutputValue(record, CommittingTransactionsRecord::getEpoch)).append("\n");
        responseBuilder.append("Transactions to commit: ")
                .append(tryOutputValue(record, CommittingTransactionsRecord::getTransactionsToCommit)).append("\n");

        try {
            if (record.isRollingTxnRecord()) {
                responseBuilder.append("Rolling Transaction, active epoch: ")
                        .append(tryOutputValue(record, CommittingTransactionsRecord::getCurrentEpoch)).append("\n");
            }
        } catch (Exception e) {
            responseBuilder.append("\n");
        }

        return responseBuilder.toString();
    }

    private static <T> String tryOutputValue(final T record, final Function<T, Object> getFunc) {
        StringBuilder responseBuilder = new StringBuilder();
        try {
            responseBuilder.append(getFunc.apply(record));
            return  responseBuilder.toString();

        } catch (Exception e) {
            return responseBuilder.toString();
        }
    }
}
