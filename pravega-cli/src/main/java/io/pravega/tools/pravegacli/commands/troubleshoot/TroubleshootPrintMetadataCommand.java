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

import io.pravega.common.Exceptions;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.records.*;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.getEpochIfExists;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.getHistoryTimeSeriesRecordIfExists;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.*;

/**
 * A command to print all the stream-specific metadata of the given stream.
 */
public class TroubleshootPrintMetadataCommand extends TroubleshootCommandHelper {

    protected StreamMetadataStore store;

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public TroubleshootPrintMetadataCommand(CommandArgs args) {
        super(args);
    }

    /**
     * Method to print all the stream specific metadata. The records printed in order:
     *
     * - StreamConfigurationRecord
     * - StreamTruncationRecord
     * - The set of EpochRecords along with their corresponding HistoryTimeSeriesRecords
     * - EpochTransitionRecord
     * - CommittingTransactionsRecord
     */
    @Override
    public void execute() {
        checkTroubleshootArgs();
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, Set<Fault>> faults = new HashMap<>();
        StringBuilder responseBuilder = new StringBuilder();

        try {
            ScheduledExecutorService executor = getCommandArgs().getState().getExecutor();
            store=createMetadataStore(executor);

            // The StreamConfigurationRecord.
            StreamConfigurationRecord configurationRecord = store.getConfigurationRecord(scope, streamName, null, executor)
                    .handle((x, e) -> {
                        if (e != null) {
                            if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                                return null;
                            } else {
                                throw new CompletionException(e);
                            }
                        }
                        return x.getObject();
                    }).join();

            responseBuilder.append("StreamConfigurationRecord: ").append("\n");
            responseBuilder.append(outputConfiguration(configurationRecord)).append("\n");

            // The StreamTruncationRecord.
            StreamTruncationRecord truncationRecord = store.getTruncationRecord(scope, streamName, null, executor)
                    .handle((x, e) -> {
                        if (e != null) {
                            if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                                return null;
                            } else {
                                throw new CompletionException(e);
                            }
                        }
                        return x.getObject();
                    }).join();

            responseBuilder.append("StreamTruncationRecord: ").append("\n");
            if (truncationRecord!= null && truncationRecord.equals(StreamTruncationRecord.EMPTY)) {
                responseBuilder.append("EMPTY\n").append("\n");
            } else {
                responseBuilder.append(outputTruncation(truncationRecord)).append("\n");
            }

            // The epoch and history records.
            responseBuilder.append("EpochRecords and HistoryTimeSeriesRecords: ").append("\n");

            int epoch = 0;
            int activeEpoch = store.getActiveEpoch(scope, streamName, null, true, executor).join().getEpoch();

            while(true) {
                EpochRecord epochRecord = getEpochIfExists(store, executor, scope, streamName, epoch, faults);
                HistoryTimeSeriesRecord historyTimeSeriesRecord = getHistoryTimeSeriesRecordIfExists(store, executor, scope, streamName, epoch, faults);

                if (epoch > activeEpoch && epochRecord == null && historyTimeSeriesRecord == null) {
                    break;
                }

                responseBuilder.append("EpochRecord ").append(epoch).append(": ").append("\n");
                responseBuilder.append(outputEpoch(epochRecord)).append("\n");
                responseBuilder.append("HistoryTimeSeriesRecord ").append(epoch).append(": ").append("\n");
                responseBuilder.append(outputHistoryRecord(historyTimeSeriesRecord)).append("\n");

                epoch++;
            }

            // The EpochTransitionRecord.
            EpochTransitionRecord transitionRecord = store.getEpochTransition(scope, streamName, null, executor)
                    .handle((x, e) -> {
                        if (e != null) {
                            if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                                return null;
                            } else {
                                throw new CompletionException(e);
                            }
                        }
                        return x.getObject();
                    }).join();

            responseBuilder.append("EpochTransitionRecord: ").append("\n");
            if (transitionRecord!= null && transitionRecord.equals(EpochTransitionRecord.EMPTY)) {
                responseBuilder.append("EMPTY\n").append("\n");
            } else {
                responseBuilder.append(outputTransition(transitionRecord)).append("\n");
            }

            // The CommittingTransactionsRecord.
            CommittingTransactionsRecord committingRecord = store.getVersionedCommittingTransactionsRecord(scope, streamName, null, executor)
                    .handle((x, e) -> {
                        if (e != null) {
                            if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                                return null;
                            } else {
                                throw new CompletionException(e);
                            }
                        }
                        return x.getObject();
                    }).join();

            responseBuilder.append("CommittingTransactionsRecord: ").append("\n");
            if (committingRecord!= null && committingRecord.equals(CommittingTransactionsRecord.EMPTY)) {
                responseBuilder.append("EMPTY\n").append("\n");
            } else {
                responseBuilder.append(outputCommittingTransactions(committingRecord)).append("\n");
            }

            outputToFile(responseBuilder.toString());

        } catch (CompletionException e) {
            System.err.println("Exception during process: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
        }
    }

    public static Command.CommandDescriptor descriptor() {
        return new Command.CommandDescriptor(COMPONENT, "print-metadata", "print the stream-specific metadata",
                new Command.ArgDescriptor("scope-name", "Name of the scope"),
                new Command.ArgDescriptor("stream-name", "Name of the stream"),
                new Command.ArgDescriptor("output-file", "(OPTIONAL) The file to output the results to"));
    }
}
