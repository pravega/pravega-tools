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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

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
    public Map<Record, List<Fault>> check(ExtendedStreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, List<Fault>> faults = new HashMap<>();

        StreamConfigurationRecord configurationRecord;

        try {
            configurationRecord = store.getConfigurationRecord(scope, streamName, null, executor)
                    .thenApply(VersionedMetadata::getObject).join();

        } catch (StoreException.DataNotFoundException e) {
            Record<StreamConfigurationRecord> streamConfigurationRecord = new Record<>(null, StreamConfigurationRecord.class);
            List<Fault> faultList = new ArrayList<>();

            faultList.add(Fault.unavailable("StreamConfigurationRecord is corrupted or unavailable"));
            faults.putIfAbsent(streamConfigurationRecord, faultList);

            return faults;
        }

        Record<StreamConfigurationRecord> streamConfigurationRecord = new Record<>(configurationRecord, StreamConfigurationRecord.class);
        List<Fault> faultList = new ArrayList<>();

        faultList.add(Fault.inconsistent(streamConfigurationRecord,
                "StreamConfigurationRecord consistency check requires human intervention"));
        faults.putIfAbsent(streamConfigurationRecord, faultList);

        return faults;
    }
}
