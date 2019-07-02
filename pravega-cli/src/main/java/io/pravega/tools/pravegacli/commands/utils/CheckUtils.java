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

import io.pravega.common.Exceptions;
import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.tools.pravegacli.commands.troubleshoot.Fault;
import io.pravega.tools.pravegacli.commands.troubleshoot.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A helper class to the general checkup case.
 */
public class CheckUtils {

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
    public static Map<Record, Set<Fault>> checkConsistency(final EpochRecord record,
                                                           final HistoryTimeSeriesRecord history,
                                                           final String scope,
                                                           final String streamName,
                                                           final ExtendedStreamMetadataStore store,
                                                           final ScheduledExecutorService executor) {
        Map<Record, Set<Fault>> faults = new HashMap<>();

        if (record == null || history == null) {
            return faults;
        }

        Record<EpochRecord> epochRecord = new Record<>(record, EpochRecord.class);
        Record<HistoryTimeSeriesRecord> historyTimeSeriesRecord = new Record<>(history, HistoryTimeSeriesRecord.class);
        boolean exists;

        // Similar fields should have similar values.
        // Epoch.
        exists = checkField(record, history, "epoch value", EpochRecord::getEpoch, HistoryTimeSeriesRecord::getEpoch, faults);

        if (exists && record.getEpoch() != history.getEpoch()) {
            putInFaultMap(faults, epochRecord, Fault.inconsistent(historyTimeSeriesRecord,
                    "Epoch mismatch : May or may not be the correct record."));
        }

        // Reference Epoch
        exists = checkField(record, history, "reference epoch value", EpochRecord::getReferenceEpoch, HistoryTimeSeriesRecord::getReferenceEpoch, faults);

        if (exists && record.getReferenceEpoch() != history.getReferenceEpoch()) {
            putInFaultMap(faults, epochRecord, Fault.inconsistent(historyTimeSeriesRecord,
                    "Reference epoch mismatch."));
        }

        // Segment data
        boolean segmentExists = checkField(record, history, "segment data", EpochRecord::getSegments, HistoryTimeSeriesRecord::getSegmentsCreated, faults);

        if (segmentExists && !record.getSegments().equals(history.getSegmentsCreated())) {
            putInFaultMap(faults, epochRecord, Fault.inconsistent(historyTimeSeriesRecord,
                    "Segment data mismatch."));
        }

        // Creation time
        exists = checkField(record, history, "creation time", EpochRecord::getCreationTime, HistoryTimeSeriesRecord::getScaleTime, faults);

        if (exists && record.getCreationTime() != history.getScaleTime()) {
            putInFaultMap(faults, epochRecord, Fault.inconsistent(historyTimeSeriesRecord,
                    "Creation time mismatch."));
        }

        // Segments in the history record should be sealed.
        boolean sealedExists = checkCorrupted(history, HistoryTimeSeriesRecord::getSegmentsSealed,
                "sealed segment data", "HistoryTimeSeriesRecord", faults);

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
                    putInFaultMap(faults, historyTimeSeriesRecord, Fault.inconsistent(historyTimeSeriesRecord,
                            "Fault among the HistoryTimeSeriesRecord and the SealedSegmentRecords."));
                    break;
                }
            }
        }

        // Segments created in epoch should be ahead of the sealed segments.
        if (sealedExists && segmentExists) {
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
                putInFaultMap(faults, epochRecord, Fault.inconsistent(historyTimeSeriesRecord,
                        "EpochRecord's segments behind the sealed segments."));
            }
        }

        return faults;
    }

    private static boolean checkField(final EpochRecord record, final HistoryTimeSeriesRecord history, final String field,
                                      final Function<EpochRecord, Object> epochFunc, final Function<HistoryTimeSeriesRecord, Object> historyFunc,
                                      final Map<Record, Set<Fault>> faultMap) {
        boolean epochValExists = checkCorrupted(record, epochFunc, field, "EpochRecord", faultMap);
        boolean historyValExists = checkCorrupted(history, historyFunc, field, "HistoryTimeSeriesRecord", faultMap);

        return epochValExists && historyValExists;
    }

    public static <T> boolean checkCorrupted(final T record, final Function<T, Object> getFunc, final String field,
                                           final String className, final Map<Record, Set<Fault>> faultMap) {
        try {
            getFunc.apply(record);
        } catch (StoreException.DataNotFoundException e) {
            Record<T> tRecord = new Record<>(record, record.getClass());
            putInFaultMap(faultMap, tRecord, Fault.unavailable(className + " is missing " + field + "."));
            return false;
        }

        return true;
    }

    public static EpochRecord getEpochIfExists(final ExtendedStreamMetadataStore store, final ScheduledExecutorService executor,
                                               final String scope, final String streamName, final int epoch, final Map<Record, Set<Fault>> faultMap) {
        return store.getEpoch(scope, streamName, epoch, null, executor)
                .handle((x, e) -> {
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                            Record<EpochRecord> epochRecord = new Record<>(null, EpochRecord.class);
                            putInFaultMap(faultMap, epochRecord,
                                    Fault.unavailable("Epoch: "+ epoch + ", The corresponding EpochRecord is corrupted or does not exist."));
                        } else {
                            throw new CompletionException(e);
                        }
                    }
                    return x;
                }).join();
    }

    public static HistoryTimeSeriesRecord getHistoryTimeSeriesRecordIfExists(final ExtendedStreamMetadataStore store, final ScheduledExecutorService executor,
                                                                             final String scope, final String streamName, final int epoch, final Map<Record, Set<Fault>> faultMap) {
        return store.getHistoryTimeSeriesRecord(scope, streamName, epoch, null, executor)
                .handle((x, e) -> {
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof  StoreException.DataNotFoundException) {
                            Record<HistoryTimeSeriesRecord> historyTimeSeriesRecord = new Record<>(null, HistoryTimeSeriesRecord.class);
                            putInFaultMap(faultMap, historyTimeSeriesRecord,
                                    Fault.unavailable("History: "+ epoch + ", The corresponding HistoryTimeSeriesRecord is corrupted or does not exist."));
                        } else {
                            throw new CompletionException(e);
                        }
                    }
                    return x;
                }).join();
    }

    public static void putInFaultMap(final Map<Record, Set<Fault>> faultMap, final Record record, final Fault fault) {
        if (faultMap.containsKey(record)) {
            faultMap.get(record).add(fault);

        } else {
            Set<Fault> faultList = new HashSet<>();
            faultList.add(fault);

            faultMap.putIfAbsent(record, faultList);
        }
    }

    public static void putAllInFaultMap(final Map<Record, Set<Fault>> faultMap, final Map<Record, Set<Fault>> extraMap) {
        extraMap.forEach((k, v) -> v.forEach(fault -> putInFaultMap(faultMap, k, fault)));
    }
}
