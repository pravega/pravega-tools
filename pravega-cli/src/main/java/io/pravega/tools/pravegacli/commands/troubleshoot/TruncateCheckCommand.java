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
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamStoreFactoryExtended;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.checkCorrupted;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putInFaultMap;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputFaults;

/**
 * A helper class that checks the stream with respect to the truncate case.
 */
public class TruncateCheckCommand extends TroubleshootCommand implements Check {

    protected ExtendedStreamMetadataStore store;

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public TruncateCheckCommand(CommandArgs args) { super(args); }

    @Override
    public void execute() {
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

            Map<Record, Set<Fault>> faults = check(store, executor);
            output(outputFaults(faults));

        } catch (CompletionException e) {
            System.err.println("Exception during process: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
        }
    }

    @Override
    public Map<Record, Set<Fault>> check(ExtendedStreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, Set<Fault>> faults = new HashMap<>();

        StreamTruncationRecord truncationRecord = store.getTruncationRecord(scope, streamName, null, executor)
                .handle((x, e) -> {
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                            Record<StreamTruncationRecord> streamTruncationRecord = new Record<>(null, StreamTruncationRecord.class);
                            putInFaultMap(faults, streamTruncationRecord,
                                    Fault.unavailable("StreamTruncationRecord is corrupted or unavailable"));
                            return null;
                        } else {
                            throw new CompletionException(e);
                        }
                    }
                    return x.getObject();
                }).join();

        if (truncationRecord == null) {
            return faults;
        }

        // If the StreamTruncationRecord is EMPTY then there's no need to check further.
        if (truncationRecord.equals(StreamTruncationRecord.EMPTY)) {
            return faults;
        }

        Record<StreamTruncationRecord> streamTruncationRecord = new Record<>(truncationRecord, StreamTruncationRecord.class);

        // Need to check internal consistency.
        // Updating and segments to delete check.
        boolean updatingExists = checkCorrupted(truncationRecord, StreamTruncationRecord::isUpdating,
                "updating", "StreamTruncationRecord", faults);
        boolean toDeleteExists = checkCorrupted(truncationRecord, StreamTruncationRecord::getToDelete,
                "segments to delete", "StreamTruncationRecord", faults);

        if (updatingExists && toDeleteExists) {
            if (!truncationRecord.isUpdating()) {
                if (!truncationRecord.getToDelete().isEmpty()) {
                    putInFaultMap(faults, streamTruncationRecord, Fault.inconsistent(streamTruncationRecord,
                            "StreamTruncationRecord inconsistency in regards to updating and segments to delete"));
                }
            }
        }

        // Correct segments deletion check.
        boolean streamCutExists = checkCorrupted(truncationRecord, StreamTruncationRecord::getStreamCut,
                "stream cut", "StreamTruncationRecord", faults);
        boolean deletedExists = checkCorrupted(truncationRecord, StreamTruncationRecord::getDeletedSegments,
                "deleted segments", "StreamTruncationRecord", faults);


        if (streamCutExists && deletedExists && toDeleteExists) {
            Long streamCutMaxSegment = Collections.max(truncationRecord.getStreamCut().keySet());
            Set<Long> allDelete = truncationRecord.getToDelete();
            allDelete.addAll(truncationRecord.getDeletedSegments());

            List<Long> badSegments = allDelete.stream()
                    .filter(segment -> segment >= streamCutMaxSegment)
                    .collect(Collectors.toList());

            if (!badSegments.isEmpty()) {
                putInFaultMap(faults, streamTruncationRecord, Fault.inconsistent(streamTruncationRecord,
                        "Fault in the StreamTruncationRecord in regards to segments deletion, " +
                                "segments ahead of stream cut being deleted"));
            }
        }

        return faults;
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "truncate-check", "check health of the truncate workflow",
                new ArgDescriptor("scope-name", "Name of the scope"),
                new ArgDescriptor("stream-name", "Name of the stream"));
    }
}
