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
import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.checkConsistency;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putAllInFaultMap;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putInFaultMap;

/**
 * A helper class that checks the stream with respect to the general case.
 */
public class GeneralCheck extends TroubleshootCommand implements Check {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public GeneralCheck(CommandArgs args) { super(args); }

    @Override
    public void execute() {

    }

    @Override
    public Map<Record, List<Fault>> check(ExtendedStreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, List<Fault>> faults = new HashMap<>();

        HistoryTimeSeries history;

        // Get the HistoryTimeSeries chunk.
        try {
            history = store.getHistoryTimeSeriesChunkRecent(scope, streamName, null, executor).join();

        } catch (StoreException.DataNotFoundException e) {
            Record<HistoryTimeSeries> historySeriesRecord = new Record<>(null, HistoryTimeSeries.class);
            putInFaultMap(faults, historySeriesRecord,
                    Fault.unavailable("HistoryTimeSeries chunk is corrupted or unavailable"));

            return faults;
        }

        ImmutableList<HistoryTimeSeriesRecord> historyRecords = history.getHistoryRecords();

        // Check the relation between each EpochRecord and its corresponding HistoryTimeSeriesRecord.
        for (HistoryTimeSeriesRecord historyRecord : historyRecords.reverse()) {
            EpochRecord correspondingEpochRecord;

            try {
                correspondingEpochRecord = store.getEpoch(scope, streamName, historyRecord.getEpoch(),
                        null, executor).join();

            } catch (StoreException.DataNotFoundException e) {
                Record<EpochRecord> epochRecord = new Record<>(null, EpochRecord.class);
                putInFaultMap(faults, epochRecord,
                        Fault.unavailable("Epoch: "+ historyRecord.getEpoch() + ", The corresponding EpochRecord is corrupted or does not exist."));

                continue;
            }

            putAllInFaultMap(faults, checkConsistency(correspondingEpochRecord, historyRecord, scope, streamName, store, executor));
        }

        return faults;
    }
}
