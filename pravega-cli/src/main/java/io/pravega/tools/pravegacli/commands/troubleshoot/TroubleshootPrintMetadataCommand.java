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
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamStoreFactoryExtended;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.getEpochIfExists;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.getHistoryTimeSeriesRecordIfExists;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputCommittingTransactions;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputConfiguration;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputEpoch;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputHistoryRecord;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputTransition;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputTruncation;

public class TroubleshootPrintMetadataCommand extends TroubleshootCommand {

    protected ExtendedStreamMetadataStore store;

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public TroubleshootPrintMetadataCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, Set<Fault>> faults = new HashMap<>();

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

            output("StreamConfigurationRecord: ");
            output(outputConfiguration(configurationRecord));

            output("\n");

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

            output("StreamTruncationRecord: ");
            output(outputTruncation(truncationRecord));

            output("\n");

            output("EpochRecords and HistoryTimeSeriesRecords: ");

            int epoch = 0;
            int activeEpoch = store.getActiveEpoch(scope, streamName, null, true, executor).join().getEpoch();

            while(true) {
                EpochRecord epochRecord = getEpochIfExists(store, executor, scope, streamName, epoch, faults);
                HistoryTimeSeriesRecord historyTimeSeriesRecord = getHistoryTimeSeriesRecordIfExists(store, executor, scope, streamName, epoch, faults);

                if (epoch > activeEpoch && epochRecord == null && historyTimeSeriesRecord == null) {
                    break;
                }

                output("EpochRecord" + epoch + ": ");
                output(outputEpoch(epochRecord));
                output("HistoryTimeSeriesRecord" + epoch + ": ");
                output(outputHistoryRecord(historyTimeSeriesRecord));

                epoch++;
            }

            output("\n");

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

            output("EpochTransitionRecord: ");
            output(outputTransition(transitionRecord));

            output("\n");

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

            output("CommittingTransactionsRecord: ");
            output(outputCommittingTransactions(committingRecord));

            output("\n");

        } catch (CompletionException e) {
            System.err.println("Exception during process: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "print-metadata", "print the stream-specific metadata",
                new ArgDescriptor("scope-name", "Name of the scope"),
                new ArgDescriptor("stream-name", "Name of the stream"));
    }
}
