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
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamStoreFactoryExtended;
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

import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.checkConsistency;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.checkCorrupted;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.getEpochIfExists;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putAllInFaultMap;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putInFaultMap;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputFaults;

/**
 * A helper class that checks the stream with respect to the general case.
 */
public class GeneralCheckCommand extends TroubleshootCommand implements Check {

    protected ExtendedStreamMetadataStore store;

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public GeneralCheckCommand(CommandArgs args) { super(args); }

    @Override
    public void execute() {
        try {
            @Cleanup
            CuratorFramework zkClient = createZKClient();
            ScheduledExecutorService executor = getCommandArgs().getState().getExecutor();

            SegmentHelper segmentHelper;
            if (getCLIControllerConfig().getMetadataBackend().equals(CLIControllerConfig.MetadataBackends.ZOOKEEPER.name())) {
                store = StreamStoreFactoryExtended.createZKStore(zkClient, executor);
            } else {
                segmentHelper = instantiateSegmentHelper(zkClient);
                AuthHelper authHelper = AuthHelper.getDisabledAuthHelper();
                store = StreamStoreFactoryExtended.createPravegaTablesStore(segmentHelper, authHelper, zkClient, executor);
            }

            Map<Record, Set<Fault>> faults = check(store, executor);
            output(outputFaults(faults));

        } catch (CompletionException e) {
            System.err.println("Exception during process: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
        }
    }

    @Override
    public Map<Record, Set<Fault>> check(ExtendedStreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, Set<Fault>> faults = new HashMap<>();

        // Get the HistoryTimeSeries chunk.
        HistoryTimeSeries history = store.getHistoryTimeSeriesChunkRecent(scope, streamName, null, executor)
                .handle((x, e) -> {
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                            Record<HistoryTimeSeries> historySeriesRecord = new Record<>(null, HistoryTimeSeries.class);
                            putInFaultMap(faults, historySeriesRecord,
                                    Fault.unavailable("HistoryTimeSeries chunk is corrupted or unavailable"));
                            return null;
                        } else {
                            throw new CompletionException(e);
                        }
                    }
                    return x;
                }).join();

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
                new ArgDescriptor("stream-name", "Name of the stream"));
    }
}
