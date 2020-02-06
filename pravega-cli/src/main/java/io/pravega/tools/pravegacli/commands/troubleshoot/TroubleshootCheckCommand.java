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

import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putAllInFaultMap;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputFaults;

/**
 * Runs a diagnosis of the stream while providing pointers and highlighting faults when found.
 */
public class TroubleshootCheckCommand extends TroubleshootCommandHelper {


    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public TroubleshootCheckCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {


        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, Set<Fault>> faults = new HashMap<>();
        try {
            executor = getCommandArgs().getState().getExecutor();
            store=getStreamMetaDataStore(executor);
            ScaleCheckCommand scale = new ScaleCheckCommand(getCommandArgs());
            CommittingTransactionsCheckCommand committingTransactions = new CommittingTransactionsCheckCommand(getCommandArgs());
            TruncateCheckCommand truncate = new TruncateCheckCommand(getCommandArgs());
            UpdateCheckCommand update = new UpdateCheckCommand(getCommandArgs());
            GeneralCheckCommand general = new GeneralCheckCommand(getCommandArgs());

            // The Update Checkup.
            Map<Record, Set<Fault>> updateFaults = update.check(store, executor);
            System.out.println("check 4");
            // The General Checkup.
            if (runCheckup(faults, updateFaults, general::check, executor, "General Checkup")) {
                return;
            }
            System.out.println("check 5");
            // Check for viability of workflow check up.
            int currentEpoch = store.getActiveEpoch(scope, streamName, null, true, executor).join().getEpoch();
            int historyCurrentEpoch = store.getHistoryTimeSeriesChunk(scope, streamName, (currentEpoch/ HistoryTimeSeries.HISTORY_CHUNK_SIZE),null, executor).join().getLatestRecord().getEpoch();
            System.out.println("historyCurrentEpoch" + historyCurrentEpoch);
            System.out.println("check 6");

            if (currentEpoch != historyCurrentEpoch) {
                // The Scale Checkup.
                if (runCheckup(faults, updateFaults, scale::check, executor, "Scale Checkup")) {
                    return;
                }

                // The Committing Transactions Checkup.
                System.out.println("check 7");
                if (runCheckup(faults, updateFaults, committingTransactions::check, executor, "Committing_txn Checkup")) {
                    return;
                }
            }

            // The Truncate Checkup.
            if (runCheckup(faults, updateFaults, truncate::check, executor, "Truncate Checkup")) {
                return;
            }


            output(outputFaults(updateFaults));
            output("Everything seems OK.\n");

        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
        }
    }

    public ScheduledExecutorService  getExcecutor(){
        return getCommandArgs().getState().getExecutor();
    }
    public StreamMetadataStore getStreamMetaDataStore(ScheduledExecutorService executor){
        return createMetadataStore(executor);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "diagnosis", "check health based on stream-specific metadata",
                new ArgDescriptor("scope-name", "Name of the scope"),
                new ArgDescriptor("stream-name", "Name of the stream"));
    }

    private boolean runCheckup(final Map<Record, Set<Fault>> faults, final Map<Record, Set<Fault>> updateFaults,
                               final BiFunction<StreamMetadataStore, ScheduledExecutorService, Map<Record, Set<Fault>>> check,
                               final ScheduledExecutorService executor, final String checkupName ) {
        try {
            putAllInFaultMap(faults, check.apply(store, executor));

            if (!faults.isEmpty()) {
                putAllInFaultMap(faults, updateFaults);
                output(outputFaults(faults));
                return true;
            }

            return false;

        } catch (Exception e) {
            output(checkupName + " error: " + e.getMessage());
            return false;
        }
    }


}
