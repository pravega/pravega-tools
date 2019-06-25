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

public class EpochHistoryCrossCheck {

    public static boolean checkConsistency(EpochRecord record, HistoryTimeSeriesRecord history, String scope, String streamName,
                                           ExtendedStreamMetadataStore store, ScheduledExecutorService executor) {
        StringBuilder responseBuilder = new StringBuilder();
        boolean isConsistent;

        if (record == null || history == null) {
            return false;
        }

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
