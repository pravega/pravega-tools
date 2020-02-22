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
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.checkCorrupted;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putInFaultMap;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputFaults;

/**
 * A helper class that checks the stream with respect to the truncate case.
 */
public class TruncateCheckCommand extends TroubleshootCommandHelper implements Check {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public TruncateCheckCommand(CommandArgs args) { super(args); }

    /**
     * The method to execute the check method as part of the execution of the command.
     */
    @Override
    public void execute() {
        checkTroubleshootArgs();
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
            outputToFile(outputFaults(faults));

        } catch (CompletionException e) {
            System.err.println("Exception during process: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
        }
    }

    /**
     * Method to check the consistency of the stream with respect to truncating workflow. We first obtain the StreamTruncationRecord
     * and then run the following checks:
     *
     * - If the StreamTruncationRecord is not EMPTY then we check for the internal consistency of the StreamTruncationRecord.
     *
     * - Firstly the updating flag cannot be set to false if the segments to delete list is not empty because the updating
     *   flag being false indicates the completion of the truncation workflow during which the segments to delete should be
     *   empty as the workflow has ended.
     *
     * - We run a check to make sure that there are no segments deleted ahead of the stream cut. This includes checking both
     *   the deleted segments and the segments to delete.
     *
     * Any faults which are noticed are immediately recorded and then finally returned.
     *
     * @param store     an instance of the Stream metadata store
     * @param executor  callers executor
     * @return A map of Record and a set of Faults associated with it.
     */
    @Override
    public Map<Record, Set<Fault>> check(StreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, Set<Fault>> faults = new HashMap<>();
        StreamTruncationRecord truncationRecord=null;
        try {
            truncationRecord = store.getTruncationRecord(scope, streamName, null, executor).
                    thenApply(VersionedMetadata::getObject).join();

        } catch (CompletionException completionException ) {
            if (Exceptions.unwrap(completionException) instanceof StoreException.DataNotFoundException) {
                Record<StreamTruncationRecord> streamTruncationRecord = new Record<>(null, StreamTruncationRecord.class);
                putInFaultMap(faults, streamTruncationRecord,
                        Fault.unavailable("StreamTruncationRecord is corrupted or unavailable"));

                return faults;
            }
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

        if (updatingExists && toDeleteExists && truncationRecord.isUpdating()==false) {
            if (!truncationRecord.isUpdating()) {
                if (!truncationRecord.getToDelete().isEmpty()) {
                    putInFaultMap(faults, streamTruncationRecord, Fault.inconsistent(streamTruncationRecord,
                            "StreamTruncationRecord inconsistency in regards to updating and segments to delete"));
                    return faults;
                }
            }
        }


        // Correct segments deletion check.
        boolean streamCutExists = checkCorrupted(truncationRecord, StreamTruncationRecord::getStreamCut,
                "stream cut", "StreamTruncationRecord", faults);
        boolean deletedExists = checkCorrupted(truncationRecord, StreamTruncationRecord::getDeletedSegments,
                "deleted segments", "StreamTruncationRecord", faults);


        if (streamCutExists && deletedExists && toDeleteExists) {

            Long streamCutMaxSegment;
            if(truncationRecord.getStreamCut().size()!=0)
            streamCutMaxSegment = Collections.max(truncationRecord.getStreamCut().keySet());
            else
                streamCutMaxSegment=0L;
            Set<Long> allDelete = truncationRecord.getToDelete();
            if(truncationRecord.getDeletedSegments().size()!=0)
             allDelete.addAll(truncationRecord.getDeletedSegments());

            if(allDelete.size()>0) {
                List<Long> badSegments = allDelete.stream()
                        .filter(segment -> segment >= streamCutMaxSegment)
                        .collect(Collectors.toList());

                if (!badSegments.isEmpty()) {
                    putInFaultMap(faults, streamTruncationRecord, Fault.inconsistent(streamTruncationRecord,
                            "Fault in the StreamTruncationRecord in regards to segments deletion, " +
                                    "segments ahead of stream cut being deleted"));
                }
            }
        }

        return faults;
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "truncate-check", "check health of the truncate workflow",
                new ArgDescriptor("scope-name", "Name of the scope"),
                new ArgDescriptor("stream-name", "Name of the stream"),
                new ArgDescriptor("output-file", "(OPTIONAL) The file to output the results to"));
    }
}
