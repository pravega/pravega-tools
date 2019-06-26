/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli.commands.troubleshoot;

import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * A helper class to the general checkup case.
 */
public class EpochHistoryCrossCheck {

    /**
     * Method to check for consistency among a given EpochRecord and its corresponding HistoryTimeSeriesRecord.
     *
     * @param record      EpochRecord
     * @param history     HistoryTimeSeriesRecord
     * @param scope       stream scope
     * @param streamName  stream name
     * @param store       an instance of the extended metadata store
     * @param executor    callers executor
     * @return
     */
    public static boolean checkConsistency(final EpochRecord record,
                                           final HistoryTimeSeriesRecord history,
                                           final String scope,
                                           final String streamName,
                                           final ExtendedStreamMetadataStore store,
                                           final ScheduledExecutorService executor) {
        StringBuilder responseBuilder = new StringBuilder();
        boolean isConsistent;

        if (record == null || history == null) {
            return false;
        }

        // Similar fields should have similar values.
        isConsistent = record.getEpoch() == history.getEpoch();
        if (record.getEpoch() != history.getEpoch()) {
            responseBuilder.append("Epoch mismatch : May or may not be the correct record").append("\n");
        }

        isConsistent = isConsistent && record.getReferenceEpoch() == history.getReferenceEpoch();
        if (record.getReferenceEpoch() != history.getReferenceEpoch()) {
            responseBuilder.append("Reference epoch mismatch.").append("\n");
        }

        isConsistent = isConsistent && record.getSegments().equals(history.getSegmentsCreated());
        if (!record.getSegments().equals(history.getSegmentsCreated())) {
            responseBuilder.append("Segment data mismatch.").append("\n");
        }

        isConsistent = isConsistent && record.getCreationTime() == history.getScaleTime();
        if (record.getCreationTime() != history.getScaleTime()) {
            responseBuilder.append("Creation time mismatch.").append("\n");
        }

        List<Long> sealedSegmentsHistory = history.getSegmentsSealed().stream()
                .map(StreamSegmentRecord::getSegmentNumber)
                .mapToLong(Integer::longValue)
                .boxed()
                .collect(Collectors.toList());

        // Segments in the history record should be sealed.
        for (Long id : sealedSegmentsHistory) {
            boolean isSealed = store.checkSegmentSealed(scope, streamName, id, null, executor).join();
            if (!isSealed) {
                responseBuilder.append("Inconsistency among the HistoryTimeSeriesRecord and the SealedSegmentRecords").append("\n");
                isConsistent = false;
                break;
            }
        }

        Long epochMinSegment = Collections.min(record.getSegments().stream()
                .map(StreamSegmentRecord::getSegmentNumber)
                .mapToLong(Integer::longValue)
                .boxed()
                .collect(Collectors.toList()));

        Long maxSealedSegment;
        if (sealedSegmentsHistory.isEmpty()) {
            maxSealedSegment = Long.MIN_VALUE;
        } else {
            maxSealedSegment = Collections.max(sealedSegmentsHistory);
        }

        // Consistency in the new segments and the sealed segments.
        if (epochMinSegment < maxSealedSegment) {
            responseBuilder.append("EpochRecord's segments behind the sealed segments.");
            isConsistent = false;
        }

        if (!isConsistent) {
            System.out.println(responseBuilder.toString());
        }
        return isConsistent;
    }
}
