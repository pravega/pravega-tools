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
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactoryExtended;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putAllInFaultMap;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputFaults;

/**
 * Runs a diagnosis of the stream while providing pointers and highlighting faults when found.
 */
public class TroubleshootCheckCommand extends TroubleshootCommand {

    protected ExtendedStreamMetadataStore store;

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public TroubleshootCheckCommand(CommandArgs args) {
        super(args);
    }

    /**
     * Method to execute the command. We run the checks in the following step by step manner:
     *
     * - We first run an update check and record any faults that might occur.
     * - Followed by a general check.
     * - We then check if the active epoch is the latest EpochRecord. If it is not then that means that we were most
     *   likely in the middle of the scale or committing transactions workflow as these two involve changing the active epoch.
     * - After which finally the truncate check is run.
     *
     * In case o any faults appearing in any of the checks we immediately stop there as the errors that get fixed in that case
     * could also fix errors that are going to occur.
     * Example: general check which identifies inconsistencies among EpochRecords and HistoryTimeSeries record may identify an error
     * that when immediately solved could also potentially solve an error that could've been spotted in the, lets say, scale workflow.
     */
    @Override
    public void execute() {
        checkTroubleshootArgs();
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

            GeneralCheckCommand general = new GeneralCheckCommand(getCommandArgs());
            ScaleCheckCommand scale = new ScaleCheckCommand(getCommandArgs());
            CommittingTransactionsCheckCommand committingTransactions = new CommittingTransactionsCheckCommand(getCommandArgs());
            TruncateCheckCommand truncate = new TruncateCheckCommand(getCommandArgs());
            UpdateCheckCommand update = new UpdateCheckCommand(getCommandArgs());

            // The Update Checkup.
            Map<Record, Set<Fault>> updateFaults = update.check(store, executor);

            // The General Checkup.
            if (runCheckup(faults, updateFaults, general::check, executor, "General Checkup")) {
                return;
            }

            // Check for viability of workflow check up.
            int currentEpoch = store.getActiveEpoch(scope, streamName, null, true, executor).join().getEpoch();
            int historyCurrentEpoch = store.getHistoryTimeSeriesChunkRecent(scope, streamName, null, executor).join().getLatestRecord().getEpoch();

            if (currentEpoch != historyCurrentEpoch) {
                // The Scale Checkup.
                if (runCheckup(faults, updateFaults, scale::check, executor, "Scale Checkup")) {
                    return;
                }

                // The Committing Transactions Checkup.
                if (runCheckup(faults, updateFaults, committingTransactions::check, executor, "Committing_txn Checkup")) {
                    return;
                }
            }

            // The Truncate Checkup.
            if (runCheckup(faults, updateFaults, truncate::check, executor, "Truncate Checkup")) {
                return;
            }

            output(outputFaults(updateFaults));
            output("Everything seems OK.");

        } catch (CompletionException e) {
            System.err.println("Exception during process: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "diagnosis", "check health based on stream-specific metadata",
                new ArgDescriptor("scope-name", "Name of the scope"),
                new ArgDescriptor("stream-name", "Name of the stream"),
                new ArgDescriptor("output-file", "(OPTIONAL) The file to output the results to"));
    }

    private boolean runCheckup(final Map<Record, Set<Fault>> faults, final Map<Record, Set<Fault>> updateFaults,
                            final BiFunction<ExtendedStreamMetadataStore, ScheduledExecutorService, Map<Record, Set<Fault>>> check,
                            final ScheduledExecutorService executor, final String checkupName) {
        try {
            putAllInFaultMap(faults, check.apply(store, executor));

            if (!faults.isEmpty()) {
                putAllInFaultMap(faults, updateFaults);
                outputToFile(outputFaults(faults));
                return true;
            }

            return false;

        } catch (Exception e) {
            output(checkupName + " error: " + e.getMessage());
            return false;
        }
    }
}
