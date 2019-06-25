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

import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputConfiguration;

public class UpdateCheck extends TroubleshootCommand implements Check {

    protected ExtendedStreamMetadataStore store;

    public UpdateCheck(CommandArgs args) { super(args); }

    @Override
    public void execute() {

    }

    @Override
    public boolean check(ExtendedStreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        StringBuilder responseBuilder = new StringBuilder();

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
            responseBuilder.append("StreamConfigurationRecord: ").append(outputConfiguration(configurationRecord));

            output(responseBuilder.toString());
            return true;
    }
}
