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
import io.micrometer.shaded.reactor.core.Exceptions;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.*;
import io.pravega.tools.pravegacli.commands.CommandArgs;

import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.*;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputFaults;

/**
 * A helper class that checks the stream with respect to the scale case.
 */
public class ScaleCheckCommand extends TroubleshootCommandHelper implements Check {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ScaleCheckCommand(CommandArgs args) {
        super(args);
    }

    /**
     * The method to execute the check method as part of the execution of the command.
     */
    @Override
    public void execute() {
        checkTroubleshootArgs();
        try {
            ScheduledExecutorService executor = getCommandArgs().getState().getExecutor();
            store = createMetadataStore(executor);
            check(store, executor);
            Map<Record, Set<Fault>> faults = check(store, executor);
            outputToFile(outputFaults(faults));
        } catch (CompletionException e) {
            System.err.println("Exception during process: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
        }
    }

    /**
     * Method to check the consistency of the stream with respect to scaling workflow. We first obtain the EpochTransitionRecord
     * and then run the following checks:
     *
     * - If the EpochTransitionRecord is not empty then we try to obtain the new epoch record as dictated by the EpochTransitionRecord.
     *
     * - Once we have the epoch record, we also obtain the corresponding history record and then run the desired consistency
     *   checks among them. If any one of the record is not available then we stop and return all the faults upto this point.
     *
     * - We check whether the segments created list in the epoch record is in line with the segments described by the new
     *   ranges in the EpochTransitionRecord.
     *
     * - We check if the segments sealed as mentioned in the HistoryTimeSeriesRecord are equivalent to the segments to be sealed
     *   as described in the EpochTransitionRecord.
     *
     * Any faults which are noticed are immediately recorded and then finally returned.
     *
     * @param store     an instance of the Stream metadata store
     * @param executor  callers executor
     * @return A map of Record and a set of Faults associated with it.
     */
    @Override
    public Map<Record, Set<Fault>> check(StreamMetadataStore store, ScheduledExecutorService executor) {
        checkTroubleshootArgs();
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, Set<Fault>> faults = new HashMap<>();

        // Check for the existence of an EpochTransitionRecord.
        EpochTransitionRecord transitionRecord = EpochTransitionRecord.EMPTY;

        // To obtain the EpochTransitionRecord and check if it is corrupted or not.
        try {
            transitionRecord = store.getEpochTransition(scope, streamName, null, executor).
                    thenApply(VersionedMetadata::getObject).join();

        } catch (CompletionException completionException) {
            if (Exceptions.unwrap(completionException) instanceof StoreException.DataNotFoundException || transitionRecord.equals(EpochTransitionRecord.EMPTY)) {
                Record<EpochTransitionRecord> epochTransitionRecord = new Record<>(null, EpochTransitionRecord.class);
                putInFaultMap(faults, epochTransitionRecord,
                        Fault.unavailable("EpochTransitionRecord is corrupted or unavailable"));

                return faults;
            }
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
                    Fault.unavailable("Epoch: " + transitionRecord.getNewEpoch() + ", The corresponding EpochRecord is corrupted or does not exist."));

            epochExists = false;
        }

        // To obtain the corresponding HistoryTimeSeriesRecord and check if it corrupted or not.
        try {

            int chunkNumber = transitionRecord.getNewEpoch() / HistoryTimeSeries.HISTORY_CHUNK_SIZE;
            neededHistoryRecord = store.getHistoryTimeSeriesChunk(scope, streamName, chunkNumber,
                    null, executor).join().getLatestRecord();

        } catch (StoreException.DataNotFoundException e) {
            Record<HistoryTimeSeriesRecord> historyTimeSeriesRecord = new Record<>(null, HistoryTimeSeriesRecord.class);
            putInFaultMap(faults, historyTimeSeriesRecord,
                    Fault.unavailable("History: " + transitionRecord.getNewEpoch() + ", The corresponding HistoryTimeSeriesRecord is corrupted or does not exist."));

            historyExists = false;
        }

        // Return the faults in case of corruption.
        if (!(epochExists && historyExists)) {
            return faults;
        }

        // Check the EpochRecord and HistoryTimeSeriesRecord.
        putAllInFaultMap(faults, checkConsistency(neededEpochRecord, neededHistoryRecord, false, scope, streamName, store, executor));

        Record<EpochTransitionRecord> epochTransitionRecord = new Record<>(transitionRecord, EpochTransitionRecord.class);
        Record<EpochRecord> epochRecord = new Record<>(neededEpochRecord, EpochRecord.class);
        Record<HistoryTimeSeriesRecord> historyTimeSeriesRecord = new Record<>(neededHistoryRecord, HistoryTimeSeriesRecord.class);

        // Check the EpochTransitionRecord with the EpochRecord and the HistoryTimeSeriesRecord.
        // Cross check the segments
        boolean getSegmentsExists = checkCorrupted(transitionRecord, EpochTransitionRecord::getNewSegmentsWithRange,
                "segments created", "EpochTransitionRecord", faults);
        if (getSegmentsExists) {
            Set<Long> segmentIds = neededEpochRecord.getSegmentIds();
            ImmutableMap<Long, Map.Entry<Double, Double>> newSegments = transitionRecord.getNewSegmentsWithRange();

            for (Long id : segmentIds) {
                if (neededEpochRecord.getSegment(id).getCreationEpoch() == neededEpochRecord.getEpoch()) {
                    SimpleEntry<Double, Double> segmentRange = new SimpleEntry<>(neededEpochRecord.getSegment(id).getKeyStart(),
                            neededEpochRecord.getSegment(id).getKeyEnd());

                    if (!segmentRange.equals(newSegments.get(id))) {
                        putInFaultMap(faults, epochTransitionRecord,
                                Fault.inconsistent(epochRecord, "EpochRecord and the EpochTransitionRecord mismatch in the segments"));
                        return faults;
                    }
                }
            }
        }


        // Cross check the sealed segments.
        boolean getSealedSegmentsExists = checkCorrupted(transitionRecord, EpochTransitionRecord::getSegmentsToSeal,
                "segments to be sealed", "EpochTransitionRecord", faults);

        if (getSealedSegmentsExists) {
            List<Long> sealedSegmentTransition = new ArrayList<>(transitionRecord.getSegmentsToSeal());
            List<Long> sealedSegmentsHistory = neededHistoryRecord.getSegmentsSealed().stream()
                    .map(StreamSegmentRecord::getSegmentNumber)
                    .mapToLong(Integer::longValue)
                    .boxed()
                    .collect(Collectors.toList());

            if (!sealedSegmentTransition.equals(sealedSegmentsHistory)) {
                putInFaultMap(faults, epochTransitionRecord,
                        Fault.inconsistent(historyTimeSeriesRecord, "HistoryTimeSeriesRecord and EpochTransitionRecord mismatch in the sealed segments"));
                return faults;
            }
        }

        return faults;
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "scale-check", "check health of the scale workflow",
                new ArgDescriptor("scope-name", "Name of the scope"),
                new ArgDescriptor("stream-name", "Name of the stream"),
                new ArgDescriptor("output-file", "(OPTIONAL) The file to output the results to"));
    }
}
