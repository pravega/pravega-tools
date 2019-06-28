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

import com.google.common.collect.ImmutableMap;
import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.checkConsistency;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.checkCorrupted;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putAllInFaultMap;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putInFaultMap;

/**
 * A helper class that checks the stream with respect to the scale case.
 */
public class ScaleCheck extends TroubleshootCommand implements Check {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ScaleCheck(CommandArgs args) { super(args); }

    @Override
    public void execute() {

    }

    @Override
    public Map<Record, List<Fault>> check(ExtendedStreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, List<Fault>> faults = new HashMap<>();

        // Check for the existence of an EpochTransitionRecord.
        EpochTransitionRecord transitionRecord;

        // To obtain the EpochTransitionRecord and check if it is corrupted or not.
        try {
            transitionRecord = store.getEpochTransition(scope, streamName, null, executor)
                    .thenApply(VersionedMetadata::getObject).join();

        } catch (StoreException.DataNotFoundException e) {
            Record<EpochTransitionRecord> epochTransitionRecord = new Record<>(null, EpochTransitionRecord.class);
            putInFaultMap(faults, epochTransitionRecord,
                    Fault.unavailable("EpochTransitionRecord is corrupted or unavailable"));

            return faults;
        }

        // If the EpochTransitionRecord is EMPTY then there's no need to check further.
        if (transitionRecord.equals(EpochTransitionRecord.EMPTY)) {
            return faults;
        }

        EpochRecord neededEpochRecord = null;
        boolean epochExists = true;
        HistoryTimeSeriesRecord neededHistoryRecord = null;
        boolean historyExists = true;

        // To obtain the corresponding EpochRecord and check if it is corrupted or not.
        try {
            neededEpochRecord = store.getEpoch(scope, streamName, transitionRecord.getNewEpoch(),
                    null, executor).join();

        } catch (StoreException.DataNotFoundException e) {
            Record<EpochRecord> epochRecord = new Record<>(null, EpochRecord.class);
            putInFaultMap(faults, epochRecord,
                    Fault.unavailable("Epoch: "+ transitionRecord.getNewEpoch() + ", The corresponding EpochRecord is corrupted or does not exist."));

            epochExists = false;
        }

        // To obtain the corresponding HistoryTimeSeriesRecord and check if it corrupted or not.
        try {
            neededHistoryRecord = store.getHistoryTimeSeriesRecord(scope, streamName, transitionRecord.getNewEpoch(),
                    null, executor).join();

        } catch (StoreException.DataNotFoundException e) {
            Record<HistoryTimeSeriesRecord> historyTimeSeriesRecord = new Record<>(null, HistoryTimeSeriesRecord.class);
            putInFaultMap(faults, historyTimeSeriesRecord,
                    Fault.unavailable("History: "+ transitionRecord.getNewEpoch() + ", The corresponding HistoryTimeSeriesRecord is corrupted or does not exist."));

            historyExists = false;
        }

        // Return the faults in case of corruption.
        if (!(epochExists && historyExists)) {
            return faults;
        }

        // Check the EpochRecord and HistoryTimeSeriesRecord.
        putAllInFaultMap(faults, checkConsistency(neededEpochRecord, neededHistoryRecord, scope, streamName, store, executor));

        Record<EpochTransitionRecord> epochTransitionRecord = new Record<>(transitionRecord, EpochTransitionRecord.class);
        Record<EpochRecord> epochRecord = new Record<>(neededEpochRecord, EpochRecord.class);
        Record<HistoryTimeSeriesRecord> historyTimeSeriesRecord = new Record<>(neededHistoryRecord, HistoryTimeSeriesRecord.class);

        // Check the EpochTransitionRecord with the EpochRecord and the HistoryTimeSeriesRecord.
        // Cross check the segments
        Fault transitionFault = checkCorrupted(transitionRecord, EpochTransitionRecord::getNewSegmentsWithRange,
                "segments created", "EpochTransitionRecord");
        if (transitionFault != null) {
            putInFaultMap(faults, epochTransitionRecord, transitionFault);
        }

        Set<Long> segmentIds = neededEpochRecord.getSegmentIds();
        ImmutableMap<Long, Map.Entry<Double, Double>> newSegments = transitionRecord.getNewSegmentsWithRange();

        for (Long id : segmentIds) {
            SimpleEntry<Double, Double> segmentRange = new SimpleEntry<>(neededEpochRecord.getSegment(id).getKeyStart(),
                    neededEpochRecord.getSegment(id).getKeyEnd());

            if (!segmentRange.equals(newSegments.get(id))) {
                putInFaultMap(faults, epochTransitionRecord,
                        Fault.inconsistent(epochRecord, "EpochRecord and the EpochTransitionRecord mismatch in the segments"));
                break;
            }
        }

        // Cross check the sealed segments.
        transitionFault = checkCorrupted(transitionRecord, EpochTransitionRecord::getSegmentsToSeal,
                "segments to be sealed", "EpochTransitionRecord");
        if (transitionFault != null) {
            putInFaultMap(faults, epochTransitionRecord, transitionFault);
        }

        List<Long> sealedSegmentTransition = new ArrayList<>(transitionRecord.getSegmentsToSeal());

        List<Long> sealedSegmentsHistory = neededHistoryRecord.getSegmentsSealed().stream()
                .map(StreamSegmentRecord::getSegmentNumber)
                .mapToLong(Integer::longValue)
                .boxed()
                .collect(Collectors.toList());

        if (!sealedSegmentTransition.equals(sealedSegmentsHistory)) {
            putInFaultMap(faults, epochTransitionRecord,
                    Fault.inconsistent(historyTimeSeriesRecord, "HistoryTimeSeriesRecord and EpochTransitionRecord mismatch in the sealed segments"));
        }

        return faults;
    }
}
