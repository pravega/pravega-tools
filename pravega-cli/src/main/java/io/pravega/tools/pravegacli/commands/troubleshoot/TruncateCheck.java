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
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputTruncation;

/**
 * A helper class that checks the stream with respect to the truncate case.
 */
public class TruncateCheck extends TroubleshootCommand implements Check {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public TruncateCheck(CommandArgs args) { super(args); }

    @Override
    public void execute() {

    }

    @Override
    public boolean check(ExtendedStreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        StringBuilder responseBuilder = new StringBuilder();

        StreamTruncationRecord truncationRecord;

        try {
            truncationRecord = store.getTruncationRecord(scope, streamName, null, executor)
                    .thenApply(VersionedMetadata::getObject).join();

        } catch (StoreException.DataNotFoundException e) {
            responseBuilder.append("StreamTruncationRecord is corrupted or unavailable").append("\n");
            output(responseBuilder.toString());
            return false;
        }

        // If the StreamTruncationRecord is EMPTY then there's no need to check further.
        if (truncationRecord.equals(StreamTruncationRecord.EMPTY)) {
            output("No error involving truncating.");
            return true;
        }

        boolean isConsistent = true;

        // Need to check internal consistency.
        // Updating and segments to delete check.
        if (!truncationRecord.isUpdating()) {
            if (!truncationRecord.getToDelete().isEmpty()) {
                responseBuilder.append("Inconsistency in the StreamTruncationRecord in regards to updating and segments to delete").append("\n");
                isConsistent = false;
            }
        }

        // Correct segments deletion check.
        Long streamCutMaxSegment = Collections.max(truncationRecord.getStreamCut().keySet());
        Set<Long> allDelete = truncationRecord.getToDelete();
        allDelete.addAll(truncationRecord.getDeletedSegments());

        List<Long> badSegments = allDelete.stream()
                .filter(segment -> segment >= streamCutMaxSegment)
                .collect(Collectors.toList());

        if (!badSegments.isEmpty()) {
            responseBuilder.append("Inconsistency in the StreamTruncationRecord in regards to segments deletion, " +
                    "segments ahead of stream cut being deleted").append("\n");
            isConsistent = false;
        }

        // Based on consistency, return all records or none.
        if (!isConsistent) {
            responseBuilder.append(outputTruncation(truncationRecord));
            output(responseBuilder.toString());
            return false;
        }

        output("Consistent with respect to truncating\n");
        return true;
    }
}
