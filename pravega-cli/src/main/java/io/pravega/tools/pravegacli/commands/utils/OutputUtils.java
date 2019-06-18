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

import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;

public class OutputUtils {

    public static String outputTransition(EpochTransitionRecord record) {
        StringBuilder responseBuilder = new StringBuilder();

        if (record == null) {
            return responseBuilder.toString();
        }

        responseBuilder.append("The active epoch: ").append(record.getActiveEpoch())
                .append(", creation time: ").append(record.getTime()).append("\n")
                .append("Segments to seal: ").append(record.getSegmentsToSeal()).append("\n");

        responseBuilder.append("New Ranges: ").append("\n");
        record.getNewSegmentsWithRange().forEach(
                (id, range) -> {
                    responseBuilder.append(id).append(" -> ");
                    responseBuilder.append("(").append(range.getKey())
                            .append(", ").append(range.getValue()).append(")").append("\n");
                });

        return responseBuilder.toString();
    }

    public static String outputEpoch(EpochRecord record) {
        StringBuilder responseBuilder = new StringBuilder();

        if (record == null) {
            return responseBuilder.toString();
        }

        responseBuilder.append("Stream epoch: ").append(record.getEpoch()).append(", creation time: ")
                .append(record.getCreationTime()).append("\n");
        responseBuilder.append("Segments in the epoch: ").append("\n");
        record.getSegments().forEach(segment -> responseBuilder.append("> ").append(segment.toString()).append("\n"));

        return responseBuilder.toString();
    }

    public static String outputHistoryRecord(HistoryTimeSeriesRecord record) {
        StringBuilder responseBuilder = new StringBuilder();

        if (record == null) {
            return responseBuilder.toString();
        }

        responseBuilder.append("Stream epoch: ").append(record.getEpoch()).append(", creation time: ")
                .append(record.getScaleTime()).append("\n");
        responseBuilder.append("Segments created: ").append("\n");
        record.getSegmentsCreated().forEach(segment -> responseBuilder.append("> ").append(segment.toString()).append("\n"));
        responseBuilder.append("Segments sealed: ").append("\n");
        record.getSegmentsSealed().forEach(segment -> responseBuilder.append("> ").append(segment.toString()).append("\n"));

        return responseBuilder.toString();
    }

    public static String outputTruncation(StreamTruncationRecord record) {
        StringBuilder responseBuilder = new StringBuilder();

        if (record == null) {
            return responseBuilder.toString();
        }

        responseBuilder.append("Stream Cut: ").append(record.getStreamCut()).append("\n");
        responseBuilder.append("Span: ").append(record.getSpan()).append("\n");
        responseBuilder.append("Deleted Segments: ").append(record.getDeletedSegments()).append("\n");
        responseBuilder.append("Segments to delete: ").append(record.getToDelete()).append("\n");
        responseBuilder.append("Size till stream cut: ").append(record.getSizeTill()).append("\n");
        responseBuilder.append("Updating: ").append(record.isUpdating()).append("\n");
        responseBuilder.append("Span epoch low: ").append(record.getSpanEpochLow()).append("\n");
        responseBuilder.append("Span epoch high: ").append(record.getSpanEpochHigh()).append("\n");

        return responseBuilder.toString();
    }
}
