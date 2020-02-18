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
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putInFaultMap;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputFaults;

/**
 * A helper class that checks the stream with respect to the update case.
 */
public class UpdateCheckCommand extends TroubleshootCommandHelper implements Check {

    protected StreamMetadataStore store;

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public UpdateCheckCommand(CommandArgs args) {
        super(args);
    }


    @Override
    public void execute() {
        try {
            @Cleanup
            CuratorFramework zkClient = createZKClient();
            ScheduledExecutorService executor = getCommandArgs().getState().getExecutor();

            SegmentHelper segmentHelper;
            if (getCLIControllerConfig().getMetadataBackend().equals(CLIControllerConfig.MetadataBackends.ZOOKEEPER.name())) {
                store = StreamStoreFactory.createZKStore(zkClient, executor);
            } else {
                segmentHelper = instantiateSegmentHelper(zkClient);
                GrpcAuthHelper authHelper = GrpcAuthHelper.getDisabledAuthHelper();
                store = StreamStoreFactory.createPravegaTablesStore(segmentHelper, authHelper, zkClient, executor);
            }

            Map<Record, Set<Fault>> faults = check(store, executor);
            output(outputFaults(faults));
            if (faults.size()==0)
                output("Everything is fine no update faults");

        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
        }
    }

    @Override
    public Map<Record, Set<Fault>> check(StreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, Set<Fault>> faults = new HashMap<>();

        StreamConfigurationRecord configurationRecord = null;

        try {
            configurationRecord = store.getConfigurationRecord(scope, streamName, null, executor).
                    thenApply(VersionedMetadata::getObject).join();

        } catch (CompletionException completionException) {
            if (Exceptions.unwrap(completionException) instanceof StoreException.DataNotFoundException) {
                StoreException.DataNotFoundException e = (StoreException.DataNotFoundException) Exceptions.unwrap(completionException);
                Record<StreamConfigurationRecord> streamConfigurationRecord = new Record<>(null, StreamConfigurationRecord.class);
                putInFaultMap(faults, streamConfigurationRecord,
                        Fault.unavailable("StreamConfigurationRecord is corrupted or unavailable"));

                return faults;
            }
        } catch (Exception e) {
            Record<StreamConfigurationRecord> streamConfigurationRecord = new Record<>(configurationRecord, StreamConfigurationRecord.class);
            putInFaultMap(faults, streamConfigurationRecord,
                    Fault.inconsistent(streamConfigurationRecord, "StreamConfigurationRecord consistency check requires human intervention"));

            return faults;
        }
        return faults;
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "update-check", "check the update mechanism",
                new ArgDescriptor("scope-name", "Name of the scope"),
                new ArgDescriptor("stream-name", "Name of the stream"));
    }
}
