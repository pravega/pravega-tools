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

import com.google.common.collect.ImmutableList;
import io.pravega.common.Exceptions;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.*;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputFaults;

/**
 * A helper class that checks the stream with respect to the general case.
 */
public class GeneralCheckCommand extends TroubleshootCommandHelper implements Check {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public GeneralCheckCommand(CommandArgs args) { super(args); }

    @Override
    public void execute() {
        checkTroubleshootArgs();
        try {
            ScheduledExecutorService executor = getCommandArgs().getState().getExecutor();
            store=createMetadataStore(executor);
            check(store, executor);
        } catch (CompletionException e) {
            System.err.println("Exception during process: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
        }
    }

    /**
     * Method to check the stream in a more general setting in the absence of a workflow. We first obtain the HistoryTimeSeries
     * and then run the following checks:
     *
     * - We first check if we can access the HistoryTimeSeriesRecords from the HistoryTimeSeries that we have just obtained.
     *
     * - We then iterate through all the HistoryTimeSeriesRecords and try to obtain their corresponding EpochRecords. If the EpochRecord
     *   is missing we put an unavailability fault and continue to the next record.
     *
     * - If the EpochRecord is available then we run the consistency checks involved among an EpochRecord and HistoryTimeSeriesRecord.
     *   For duplicate epochs when comparing with their respective history records we make sure not to compare segments as
     *   the history records segments for duplicate epochs are empty while epoch record itself will contain duplicate epochs.
     *
     * Any faults which are noticed are immediately recorded and then finally returned.
     *
     * @param store     an instance of the StreamMetadataStore
     * @param executor  callers executor
     * @return A map of Record and a set of Faults associated with it.
     */
    @Override
    public Map<Record, Set<Fault>> check(StreamMetadataStore store, ScheduledExecutorService executor) {
        checkTroubleshootArgs();
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, Set<Fault>> faults = new HashMap<>();

        HistoryTimeSeries history=null;

        try {
            int currentEpoch = store.getActiveEpoch(scope, streamName, null, true, executor).join().getEpoch();
            int chuckNumber=currentEpoch/ HistoryTimeSeries.HISTORY_CHUNK_SIZE;
            history = store.getHistoryTimeSeriesChunk(scope, streamName, chuckNumber,null, executor).join();

        } catch (CompletionException completionException ) {
            if (Exceptions.unwrap(completionException) instanceof StoreException.DataNotFoundException) {
                Record<HistoryTimeSeries> historySeriesRecord = new Record<>(null, HistoryTimeSeries.class);
                putInFaultMap(faults, historySeriesRecord,
                        Fault.unavailable("HistoryTimeSeries chunk is corrupted or unavailable"));

                return faults;
            }
        }

        if (history == null) {
            return faults;
        }

        if (!checkCorrupted(history, HistoryTimeSeries::getHistoryRecords, "history records", "HistoryTimeSeries", faults)) {
            return faults;
        }

        ImmutableList<HistoryTimeSeriesRecord> historyRecords = history.getHistoryRecords();

        // Check the relation between each EpochRecord and its corresponding HistoryTimeSeriesRecord.
        for (HistoryTimeSeriesRecord historyRecord : historyRecords.reverse()) {
            EpochRecord correspondingEpochRecord = getEpochIfExists(store, executor, scope, streamName, historyRecord.getEpoch(), faults);
            if (correspondingEpochRecord == null) {
                continue;
            }

            boolean referenceExists = checkCorrupted(correspondingEpochRecord, EpochRecord::getReferenceEpoch,
                    "reference epoch value", "EpochRecord", faults);
            boolean epochExists = checkCorrupted(correspondingEpochRecord, EpochRecord::getEpoch,
                    "epoch value", "EpochRecord", faults);

            if (referenceExists && epochExists && correspondingEpochRecord.getEpoch() != correspondingEpochRecord.getReferenceEpoch()) {
                putAllInFaultMap(faults, checkConsistency(correspondingEpochRecord, historyRecord, true, scope, streamName, store, executor));
            } else {
                putAllInFaultMap(faults, checkConsistency(correspondingEpochRecord, historyRecord, false, scope, streamName, store, executor));
            }
        }

        return faults;
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "general-check", "check health of the stream in a general sense",
                new ArgDescriptor("scope-name", "Name of the scope"),
                new ArgDescriptor("stream-name", "Name of the stream"),
                new ArgDescriptor("output-file", "(OPTIONAL) The file to output the results to"));
    }
}
