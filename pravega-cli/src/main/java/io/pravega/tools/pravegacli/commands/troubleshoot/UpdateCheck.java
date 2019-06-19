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
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputConfiguration;

public class UpdateCheck extends TroubleshootCommand {

    protected ExtendedStreamMetadataStore store;

    public UpdateCheck(CommandArgs args) { super(args); }

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

            StreamConfigurationRecord configurationRecord;

            try {
                configurationRecord = store.getConfigurationRecord(scope, streamName, null, executor)
                        .thenApply(VersionedMetadata::getObject).join();

            } catch (StoreException.DataNotFoundException e) {
                responseBuilder.append("StreamConfigurationRecord is corrupted or unavailable").append("\n");
                output(responseBuilder.toString());
                return false;
            }

            responseBuilder.append("StreamConfigurationRecord consistency check requires human intervention").append("\n");
            responseBuilder.append(outputConfiguration(configurationRecord));

            return true;

        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
            return true;
        }
    }
}
