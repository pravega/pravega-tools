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

import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.util.Config;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;

public class EpochRecordHistoryTimeSeriesCheck extends Command {

    protected StreamMetadataStore store;

    public EpochRecordHistoryTimeSeriesCheck(CommandArgs args) { super(args); }

    public void execute() {

    }

    public void check() {
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
                store = StreamStoreFactory.createZKStore(zkClient, executor);
            } else {
                segmentHelper = instantiateSegmentHelper(zkClient);
                AuthHelper authHelper = AuthHelper.getDisabledAuthHelper();
                store = StreamStoreFactory.createPravegaTablesStore(segmentHelper, authHelper, zkClient, executor);
            }

            EpochRecord activeEpoch = store.getActiveEpoch(scope, streamName, null, true, executor).join();

            responseBuilder.append("Current stream epoch: ").append(activeEpoch.getEpoch()).append(", creation time: ")
                    .append(activeEpoch.getCreationTime()).append("\n");
            responseBuilder.append("Segments in active epoch: ").append("\n");
            activeEpoch.getSegments().forEach(segment -> responseBuilder.append("> ").append(segment.toString()).append("\n"));

            output(responseBuilder.toString());

        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
        }
    }

    private SegmentHelper instantiateSegmentHelper(CuratorFramework zkClient) {
        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(true)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .containerCount(getServiceConfig().getContainerCount())
                .build();
        HostControllerStore hostStore = HostStoreFactory.createStore(hostMonitorConfig, StoreClientFactory.createZKStoreClient(zkClient));
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create((getCLIControllerConfig().getControllerGrpcURI())))
                .validateHostName(getCLIControllerConfig().isAuthEnabled())
                .credentials(new DefaultCredentials(getCLIControllerConfig().getPassword(), getCLIControllerConfig().getUserName()))
                .build();
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        return new SegmentHelper(connectionFactory, hostStore);
    }

}
