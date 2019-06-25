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

import java.util.concurrent.ScheduledExecutorService;

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

    @Override
    public void execute() {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);

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

            GeneralCheck generalChecker = new GeneralCheck(getCommandArgs());
            ScaleCheck scaleChecker = new ScaleCheck(getCommandArgs());
            CommittingTransactionsCheck committingTransactionsChecker = new CommittingTransactionsCheck(getCommandArgs());
            TruncateCheck truncateChecker = new TruncateCheck(getCommandArgs());
            UpdateCheck updateChecker = new UpdateCheck(getCommandArgs());
            boolean isConsistent;

            // THE GENERAL CHECKUP
            output("\n-------GENERAL CHECKUP-------\n\n");

            isConsistent = generalChecker.check(store, executor);
            if (!isConsistent) {
                return;
            }

            // THE UPDATE CHECKUP
            output("\n-------UPDATE CHECKUP-------\n\n");

            isConsistent = updateChecker.check(store, executor);
            if (!isConsistent) {
                return;
            }

            // Check for viability of workflow check up
            int currentEpoch = store.getActiveEpoch(scope, streamName, null,
                    true, executor).join().getEpoch();

            int historyCurrentEpoch = store.getHistoryTimeSeriesChunkRecent(scope, streamName, null, executor)
                    .join().getLatestRecord().getEpoch();

            if (currentEpoch != historyCurrentEpoch) {

                //THE SCALE CHECK
                output("\n-------SCALE CHECKUP-------\n\n");

                isConsistent = scaleChecker.check(store, executor);
                if (!isConsistent) {
                    return;
                }

                //THE COMMITTING TRANSACTIONS CHECK
                output("\n-------COMMITTING TRANSACTIONS CHECKUP-------\n\n");

                isConsistent = committingTransactionsChecker.check(store, executor);
                if (!isConsistent) {
                    return;
                }
            }

            //THE TRUNCATE CHECK
            output("\n-------TRUNCATE CHECKUP-------\n\n");

            isConsistent = truncateChecker.check(store, executor);
            if (!isConsistent) {
                return;
            }

            output("Everything seems ok.");

        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "diagnosis", "check health based on stream-specific metadata",
                new ArgDescriptor("scope-name", "Name of the scope"),
                new ArgDescriptor("stream-name", "Name of the stream"));
    }
}
