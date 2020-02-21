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

import com.google.common.collect.ImmutableList;
import io.pravega.common.Exceptions;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.tools.pravegacli.commands.troubleshoot.Fault;
import io.pravega.tools.pravegacli.commands.troubleshoot.Record;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.pravega.shared.NameUtils.computeSegmentId;

/**
 * A helper class to the general checkup case.
 */
public class CheckUtils {

    /**
     * Method to check for consistency among a given EpochRecord and its corresponding HistoryTimeSeriesRecord.
     * We check for field mismatches in the epoch, reference epoch, segments created and the creation time. We also make sure
     * that segments in the segments sealed list in the HistoryTimeSeriesRecord are actually sealed. Then we finally make sure that
     * are no sealed segments ahead of the created segments in the EpochRecord.
     *
     * @param record      EpochRecord
     * @param history     HistoryTimeSeriesRecord
     * @param isDuplicate a boolean determining if the epoch is duplicate or not
     * @param scope       stream scope
     * @param streamName  stream name
     * @param store       an instance of the extended metadata store
     * @param executor    callers executor
     * @return A map of Record and Fault.
     */
    public static Map<Record, Set<Fault>> checkConsistency(final EpochRecord record,
                                                           final HistoryTimeSeriesRecord history,
                                                           final boolean isDuplicate,
                                                           final String scope,
                                                           final String streamName,
                                                           final StreamMetadataStore store,
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

        if (!isDuplicate) {
            if (segmentExists && !record.getSegments().equals(history.getSegmentsCreated())) {
                putInFaultMap(faults, epochRecord, Fault.inconsistent(historyTimeSeriesRecord,
                        "Segment data mismatch."));
            }
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
                    .map(s -> computeSegmentId(s.getSegmentNumber(), s.getCreationEpoch()))
                    .collect(Collectors.toList());

            for (Long id : sealedSegmentsHistory) {
                Integer isSealed = store.getSegmentSealedEpoch(scope, streamName, id, null, executor).join();
                if (isSealed<0) {
                    putInFaultMap(faults, historyTimeSeriesRecord, Fault.inconsistent(historyTimeSeriesRecord,
                            "Fault among the HistoryTimeSeriesRecord and the SealedSegmentRecords."));
                    break;
                }
            }
        }

        // Segments created in epoch should be ahead of the sealed segments.
        if (sealedExists && segmentExists) {
            List<Integer> sealedSegments = history.getSegmentsSealed().stream()
                    .map(StreamSegmentRecord::getSegmentNumber)
                    .collect(Collectors.toList());

            Integer epochMinSegment = Collections.min(record.getSegments().stream()
                    .map(StreamSegmentRecord::getSegmentNumber)
                    .collect(Collectors.toList()));

            Integer maxSealedSegment;
            if (sealedSegmentsHistory.isEmpty()) {
                maxSealedSegment = Integer.MIN_VALUE;
            } else {
                maxSealedSegment = Collections.max(sealedSegments);
            }

            if (epochMinSegment < maxSealedSegment) {
                putInFaultMap(faults, epochRecord, Fault.inconsistent(historyTimeSeriesRecord,
                        "EpochRecord's segments behind the sealed segments."));
            }
        }

        return faults;
    }

    /**
     * Method to check if the field corresponding to both the EpochRecord and the HistoryTimeSeriesRecord are present.
     *
     * @param record      the EpochRecord
     * @param history     the HistoryTimeSeriesRecord
     * @param field       the name of the field
     * @param epochFunc   the getter for the EpochRecord
     * @param historyFunc the getter for the HistoryTimeSeriesRecord
     * @param faultMap    the map into which faults should be put
     * @return a boolean indicating if field is corrupted in both records or not.
     */
    private static boolean checkField(final EpochRecord record, final HistoryTimeSeriesRecord history, final String field,
                                      final Function<EpochRecord, Object> epochFunc, final Function<HistoryTimeSeriesRecord, Object> historyFunc,
                                      final Map<Record, Set<Fault>> faultMap) {
        boolean epochValExists = checkCorrupted(record, epochFunc, field, "EpochRecord", faultMap);
        boolean historyValExists = checkCorrupted(history, historyFunc, field, "HistoryTimeSeriesRecord", faultMap);

        return epochValExists && historyValExists;
    }

    /**
     * Method to check if the field described by the given getter is corrupted or not.
     *
     * @param record    the metadata record
     * @param getFunc   the getter method
     * @param field     the field in question
     * @param className the record name
     * @param faultMap  the map into which faults should be put
     * @return a boolean indicating if the field was accessible or not
     */
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

    /**
     * Method to return an EpochRecord if it exists.
     *
     * @param store      an instance of the extended metadata store
     * @param executor   callers executor
     * @param scope      stream scope
     * @param streamName stream name
     * @param epoch      the epoch
     * @param faultMap   the map into which faults should be put
     * @return the EpochRecord or null if it doesn't exist
     */
    public static EpochRecord getEpochIfExists(final StreamMetadataStore store, final ScheduledExecutorService executor,
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

    /**
     * Method to return an HistoryTimeSeriesRecord if it exists.
     *
     * @param store      an instance of the extended metadata store
     * @param executor   callers executor
     * @param scope      stream scope
     * @param streamName stream name
     * @param epoch      the epoch
     * @param faultMap   the map into which faults should be put
     * @return the HistoryTimeSeriesRecord or null if it doesn't exist
     */
    public static HistoryTimeSeriesRecord getHistoryTimeSeriesRecordIfExists(final StreamMetadataStore store, final ScheduledExecutorService executor,
                                                                             final String scope, final String streamName, final int epoch, final Map<Record, Set<Fault>> faultMap) {
        try {
            int chuckNumber = epoch / HistoryTimeSeries.HISTORY_CHUNK_SIZE;
            return store.getHistoryTimeSeriesChunk(scope, streamName, chuckNumber, null, executor).join().getLatestRecord();
        }catch (CompletionException completionException ) {
            if (Exceptions.unwrap(completionException) instanceof StoreException.DataNotFoundException) {
                Record<HistoryTimeSeries> historySeriesRecord = new Record<>(null, HistoryTimeSeries.class);
                putInFaultMap(faultMap, historySeriesRecord,
                        Fault.unavailable("HistoryTimeSeries chunk is corrupted or unavailable"));
            }
        }
        return null;
    }

    /**
     * Method to put a Fault in the given fault map under the given record
     *
     * @param faultMap the map into which faults should be put
     * @param record   the metadata record tp put the fault under
     * @param fault    the fault to put
     * @return none
     */
    public static void putInFaultMap(final Map<Record, Set<Fault>> faultMap, final Record record, final Fault fault) {
        if (faultMap.containsKey(record)) {
            faultMap.get(record).add(fault);

        } else {
            Set<Fault> faultList = new HashSet<>();
            faultList.add(fault);

            faultMap.putIfAbsent(record, faultList);
        }
    }

    /**
     * Method to merge two given fault maps
     *
     * @param faultMap the map into which faults should be put
     * @param extraMap the map from which to place the faults
     * @return none
     */
    public static void putAllInFaultMap(final Map<Record, Set<Fault>> faultMap, final Map<Record, Set<Fault>> extraMap) {
        extraMap.forEach((k, v) -> v.forEach(fault -> putInFaultMap(faultMap, k, fault)));
    }
}
