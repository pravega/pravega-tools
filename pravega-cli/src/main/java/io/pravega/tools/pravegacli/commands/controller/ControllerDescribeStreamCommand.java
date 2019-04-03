/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli.commands.controller;

import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.store.stream.ScaleMetadata;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

/**
 * Gets a description of different characteristics related to a Stream (e.g., configuration, state, active txn).
 */
public class ControllerDescribeStreamCommand extends ControllerCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerDescribeStreamCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String stream = getCommandArgs().getArgs().get(1);

        @Cleanup
        CuratorFramework zkClient = createZKClient();
        Executor executor = getCommandArgs().getState().getExecutor();
        StreamMetadataStore store = StreamStoreFactory.createZKStore(zkClient, executor);
        // Output the configuration of this Stream.
        CompletableFuture<StreamConfiguration> streamConfig = store.getConfiguration(scope, stream, null, executor);
        output("Stream configuration: " + streamConfig.join().toString());

        // Output the state for this Stream.
        output("Stream state: " + store.getState(scope, stream, true, null,
                executor).join().toString());

        // Output the total number of segments for this Stream.
        Set<Long> segments = store.getAllSegmentIds(scope, stream, null, executor).join();
        output("Total number of Stream segments: ", segments.size());

        // Check if the Stream is sealed.
        output("Is Stream sealed? " +  store.isSealed(scope, stream, null, executor).join());

        // Output the active epoch for this Stream.
        EpochRecord epochRecord = store.getActiveEpoch(scope, stream, null, true, executor).join();
        output("Current Stream epoch: " + epochRecord.getEpoch() + ", creation time: " +
                epochRecord.getCreationTime());

        // Output the active epoch for this Stream.
        output("Segments in active epoch: ");
        epochRecord.getSegments().forEach(s -> output("> " + s.toString()));

        // Output the number of active Transactions for ths Stream.
        output("Active Transactions in Stream: ");
        Map<UUID, ActiveTxnRecord> activeTxn = store.getActiveTxns(scope, stream, null,
                getCommandArgs().getState().getExecutor()).join();
        activeTxn.forEach((txnId, txnRecord) -> output("> TxnId: " + txnId + ", TxnRecord: " +
                txnRecord.toString()));

        // Output Truncation point.
        VersionedMetadata<StreamTruncationRecord> truncationRecord = store.getTruncationRecord(scope, stream,
                null, executor).join();
        output("Stream truncation record: lower epoch: " + truncationRecord.getObject().getSpanEpochLow() +
                ", high epoch: " + truncationRecord.getObject().getSpanEpochHigh() + ", deleted segments: " +
                truncationRecord.getObject().getDeletedSegments().size() + ", StreamCut: " + truncationRecord.getObject().getStreamCut().toString());

        // Output the metadata that describes all the scaling information for this Stream.
        List<ScaleMetadata> scaleMetadata = store.getScaleMetadata(scope, stream, segments.stream().min(Long::compareTo).get(),
                segments.stream().max(Long::compareTo).get(), null, executor).join();
        scaleMetadata.forEach(s -> output("> Scale time: " + s.getTimestamp() + ", splits: " + s.getSplits() +
                ", merges: " + s.getMerges() + ", segments: " + s.getSegments().stream()
                                                                       .map(segment -> String.valueOf(segment.getNumber()))
                                                                       .collect(Collectors.joining("-", "{", "}"))));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "describe-stream", "Get the details of a given Stream.",
                new ArgDescriptor("scope-name", "Name of the Scope where the Stream belongs to."),
                new ArgDescriptor("stream-name", "Name of the Stream to describe."));
    }
}
