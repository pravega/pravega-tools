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

import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putInFaultMap;

/**
 * A helper class that checks the stream with respect to the update case.
 */
public class UpdateCheck extends TroubleshootCommand implements Check {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public UpdateCheck(CommandArgs args) { super(args); }

    @Override
    public void execute() {

    }

    @Override
    public Map<Record, Set<Fault>> check(ExtendedStreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, Set<Fault>> faults = new HashMap<>();

        StreamConfigurationRecord configurationRecord;

        try {
            configurationRecord = store.getConfigurationRecord(scope, streamName, null, executor)
                    .thenApply(VersionedMetadata::getObject).join();

        } catch (StoreException.DataNotFoundException e) {
            Record<StreamConfigurationRecord> streamConfigurationRecord = new Record<>(null, StreamConfigurationRecord.class);
            putInFaultMap(faults, streamConfigurationRecord,
                    Fault.unavailable("StreamConfigurationRecord is corrupted or unavailable"));

            return faults;
        }

        Record<StreamConfigurationRecord> streamConfigurationRecord = new Record<>(configurationRecord, StreamConfigurationRecord.class);
        putInFaultMap(faults, streamConfigurationRecord,
                Fault.inconsistent(streamConfigurationRecord, "StreamConfigurationRecord consistency check requires human intervention"));

        return faults;
    }
}
