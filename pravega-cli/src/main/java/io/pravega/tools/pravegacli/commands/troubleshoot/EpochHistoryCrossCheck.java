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
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
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
     * @return A map of Record and Fault.
     */
    public static Map<Record, List<Fault>> checkConsistency(final EpochRecord record,
                                                            final HistoryTimeSeriesRecord history,
                                                            final String scope,
                                                            final String streamName,
                                                            final ExtendedStreamMetadataStore store,
                                                            final ScheduledExecutorService executor) {
        Map<Record, List<Fault>> faults = new HashMap<>();

        if (record == null || history == null) {
            return faults;
        }

        Record<EpochRecord> epochRecord = new Record<>(record, EpochRecord.class);
        Record<HistoryTimeSeriesRecord> historyTimeSeriesRecord = new Record<>(history, HistoryTimeSeriesRecord.class);
        List<Fault> epochFaultList = new ArrayList<>();
        List<Fault> historyFaultList = new ArrayList<>();

        boolean exists;

        // Similar fields should have similar values.
        // Epoch.
        exists = checkField(record, history, "epoch value",
                EpochRecord::getEpoch,
                HistoryTimeSeriesRecord::getEpoch,
                epochFaultList,
                historyFaultList);

        if (exists && record.getEpoch() != history.getEpoch()) {
            epochFaultList.add(Fault.inconsistent(historyTimeSeriesRecord,
                    "Epoch mismatch : May or may not be the correct record."));
        }

        // Reference Epoch
        exists = checkField(record, history, "reference epoch value",
                EpochRecord::getReferenceEpoch,
                HistoryTimeSeriesRecord::getReferenceEpoch,
                epochFaultList,
                historyFaultList);

        if (exists && record.getReferenceEpoch() != history.getReferenceEpoch()) {
            epochFaultList.add(Fault.inconsistent(historyTimeSeriesRecord,
                    "Reference epoch mismatch."));
        }

        // Segment data
        exists = checkField(record, history, "segment data",
                EpochRecord::getSegments,
                HistoryTimeSeriesRecord::getSegmentsCreated,
                epochFaultList,
                historyFaultList);

        if (exists && !record.getSegments().equals(history.getSegmentsCreated())) {
            epochFaultList.add(Fault.inconsistent(historyTimeSeriesRecord,
                    "Segment data mismatch."));
        }

        // Creation time
        exists = checkField(record, history, "creation time",
                EpochRecord::getCreationTime,
                HistoryTimeSeriesRecord::getScaleTime,
                epochFaultList,
                historyFaultList);

        if (exists && record.getCreationTime() != history.getScaleTime()) {
            epochFaultList.add(Fault.inconsistent(historyTimeSeriesRecord,
                    "Creation time mismatch."));
        }

        // Segments in the history record should be sealed.
        boolean sealedExists = true;

        try {
            history.getSegmentsSealed();
        } catch (StoreException.DataNotFoundException e) {
            historyFaultList.add(Fault.unavailable("HistoryTimeSeriesRecord is missing sealed segment data."));
            sealedExists = false;
        }

        List<Long> sealedSegmentsHistory = new ArrayList<>();

        if (sealedExists) {
            sealedSegmentsHistory = history.getSegmentsSealed().stream()
                    .map(StreamSegmentRecord::getSegmentNumber)
                    .mapToLong(Integer::longValue)
                    .boxed()
                    .collect(Collectors.toList());

            for (Long id : sealedSegmentsHistory) {
                boolean isSealed = store.checkSegmentSealed(scope, streamName, id, null, executor).join();
                if (!isSealed) {
                    epochFaultList.add(Fault.inconsistent(historyTimeSeriesRecord,
                            "Fault among the HistoryTimeSeriesRecord and the SealedSegmentRecords."));
                    break;
                }
            }
        }

        // Segments created in epoch should be ahead of the sealed segments.
        if (sealedExists && !epochFaultList.contains(Fault.unavailable("EpochRecord is missing segment data."))) {
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
                epochFaultList.add(Fault.inconsistent(historyTimeSeriesRecord,
                        "EpochRecord's segments behind the sealed segments."));
            }
        }

        if (!epochFaultList.isEmpty()) {
            faults.putIfAbsent(epochRecord, epochFaultList);
        }

        if (!historyFaultList.isEmpty()) {
            faults.putIfAbsent(historyTimeSeriesRecord, historyFaultList);
        }

        return faults;
    }

    private static boolean checkField(final EpochRecord record, final HistoryTimeSeriesRecord history, final String field,
                                      final Function<EpochRecord, Object> epochFunc, final Function<HistoryTimeSeriesRecord, Object> historyFunc,
                                      List<Fault> epochFaultList, List<Fault> historyFaultList) {
        boolean epochValExists = true;
        boolean historyValExists = true;

        try {
            epochFunc.apply(record);
        } catch (StoreException.DataNotFoundException e) {
            epochFaultList.add(Fault.unavailable("EpochRecord is missing "+ field + "."));
            epochValExists = false;
        }

        try {
            historyFunc.apply(history);
        } catch (StoreException.DataNotFoundException e) {
            historyFaultList.add(Fault.unavailable("HistoryTimeSeriesRecord is missing "+ field+ "."));
            historyValExists = false;
        }

        return epochValExists && historyValExists;
    }
}
