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
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.*;

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

    }

    @Override
    public Map<Record, Set<Fault>> check(StreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        String scope = getCommandArgs().getArgs().get(0);
        String streamName = getCommandArgs().getArgs().get(1);

        Map<Record, Set<Fault>> faults = new HashMap<>();

        HistoryTimeSeries history=null;

        // Get the HistoryTimeSeries chunk.
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

        ImmutableList<HistoryTimeSeriesRecord> historyRecords = history.getHistoryRecords();

        // Check the relation between each EpochRecord and its corresponding HistoryTimeSeriesRecord.
        for (HistoryTimeSeriesRecord historyRecord : historyRecords.reverse()) {
            EpochRecord correspondingEpochRecord=null;

            try {
                correspondingEpochRecord = store.getEpoch(scope, streamName, historyRecord.getEpoch(),
                        null, executor).join();

            }  catch (CompletionException completionException ) {
                if (Exceptions.unwrap(completionException) instanceof StoreException.DataNotFoundException) {
                    Record<EpochRecord> epochRecord = new Record<>(null, EpochRecord.class);
                    putInFaultMap(faults, epochRecord,
                            Fault.unavailable("Epoch: " + historyRecord.getEpoch() + ", The corresponding EpochRecord is corrupted or does not exist."));

                    continue;
                }
            }

            putAllInFaultMap(faults, checkConsistency(correspondingEpochRecord, historyRecord, scope, streamName, store, executor));
        }

        return faults;
    }
}
