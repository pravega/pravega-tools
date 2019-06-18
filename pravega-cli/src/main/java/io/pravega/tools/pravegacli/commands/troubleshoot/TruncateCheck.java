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
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamStoreFactoryExtended;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputTruncation;

public class TruncateCheck extends TroubleshootCommand {

    protected ExtendedStreamMetadataStore store;

    public TruncateCheck(CommandArgs args) { super(args); }

    @Override
    public void execute() {

    }

    public boolean check() {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        StringBuilder responseBuilder = new StringBuilder();

        try {
            @Cleanup
            CuratorFramework zkClient = createZKClient();
            ScheduledExecutorService executor = getCommandArgs().getState().getExecutor();

            SegmentHelper segmentHelper = null;
            if (getCLIControllerConfig().getMetadataBackend().equals(CLIControllerConfig.MetadataBackends.ZOOKEEPER.name())) {
                store = StreamStoreFactoryExtended.createZKStore(zkClient, executor);
            } else {
                segmentHelper = instantiateSegmentHelper(zkClient);
                AuthHelper authHelper = AuthHelper.getDisabledAuthHelper();
                store = StreamStoreFactoryExtended.createPravegaTablesStore(segmentHelper, authHelper, zkClient, executor);
            }

            StreamTruncationRecord truncationRecord = null;

            try {
                truncationRecord = store.getTruncationRecord(scope, streamName, null, executor)
                        .thenApply(VersionedMetadata::getObject).join();

            } catch (StoreException.DataNotFoundException e) {
                responseBuilder.append("StreamTruncationRecord is corrupted or unavailable").append("\n");
                output(responseBuilder.toString());
                return false;
            }

            // If the StreamTruncationRecord is EMPTY then there's no need to check further
            if (truncationRecord.equals(StreamTruncationRecord.EMPTY)) {
                output("No error involving truncating.");
                return true;
            }

            // Need to check internal consistency
            if (!truncationRecord.isUpdating()) {
                if (!truncationRecord.getToDelete().isEmpty()) {
                    responseBuilder.append("Inconsistency in the StreamTruncationRecord in regards to updating and segments to delete").append("\n");
                    responseBuilder.append(outputTruncation(truncationRecord));

                    output(responseBuilder.toString());
                    return false;
                }
            }



            output("Consistent with respect to truncating");
            return true;

        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
            return true;
        }
    }
}
