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

import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.tools.pravegacli.commands.troubleshoot.EpochHistoryCrossCheck.checkConsistency;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputEpoch;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputHistoryRecord;

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
    public boolean check(ExtendedStreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        StringBuilder responseBuilder = new StringBuilder();

        HistoryTimeSeries history;

        // Get the HistoryTimeSeries chunk.
        try {
            history = store.getHistoryTimeSeriesChunkRecent(scope, streamName, null, executor).join();

        } catch (StoreException.DataNotFoundException e) {
            responseBuilder.append("HistoryTimeSeries chunk is corrupted or unavailable").append("\n");
            output(responseBuilder.toString());

            return false;
        }

        ImmutableList<HistoryTimeSeriesRecord> historyRecords = history.getHistoryRecords();

        boolean isConsistent = true;
        boolean isAvailable = true;

        // Check the relation between each EpochRecord and its corresponding HistoryTimeSeriesRecord.
        for (HistoryTimeSeriesRecord record : historyRecords.reverse()) {
            EpochRecord correspondingEpochRecord;
            responseBuilder.append(record.getEpoch()).append("\n");

            try {
                correspondingEpochRecord = store.getEpoch(scope, streamName, record.getEpoch(),
                        null, executor).join();

            } catch (StoreException.DataNotFoundException e) {
                responseBuilder.append("The corresponding EpochRecord is corrupted or does not exist.").append("\n");
                responseBuilder.append("HistoryTimeSeriesRecord : ").append(outputHistoryRecord(record));
                isAvailable = false;

                continue;
            }

            isConsistent = isConsistent && checkConsistency(correspondingEpochRecord, record, scope, streamName, store, executor);

            // Output the record in case of inconsistency.
            if (!checkConsistency(correspondingEpochRecord, record, scope, streamName, store, executor)) {
                responseBuilder.append("EpochRecord : ").append(outputEpoch(correspondingEpochRecord));
                responseBuilder.append("HistoryTimeSeriesRecord : ").append(outputHistoryRecord(record));
            }
        }

        if (!isConsistent || !isAvailable) {
            output(responseBuilder.toString());
            return false;
        }

        output("History and Epoch data consistent.\n");
        return true;
    }
}
