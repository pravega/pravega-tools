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

import com.google.common.collect.ImmutableMap;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.stream.*;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static io.pravega.tools.pravegacli.commands.troubleshoot.EpochHistoryCrossCheck.checkConsistency;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.*;

public class ScaleCheck extends TroubleshootCommand {

    protected ExtendedStreamMetadataStore store;

    public ScaleCheck(CommandArgs args) { super(args); }

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

            // Check for the existence of an EpochTransitionRecord
            EpochTransitionRecord transitionRecord;

            // To obtain the EpochTransitionRecord and check if it is corrupted or not
            try {
                transitionRecord = store.getEpochTransition(scope, streamName, null, executor)
                        .thenApply(VersionedMetadata::getObject).join();

            } catch (StoreException.DataNotFoundException e) {
                responseBuilder.append("EpochTransitionRecord is corrupted").append("\n");
                output(responseBuilder.toString());
                return false;
            }

            // If the EpochTransitionRecord is EMPTY then there's no need to check further
            if (transitionRecord.equals(EpochTransitionRecord.EMPTY)) {
                output("No error involving scaling.");
                return true;
            }

            EpochRecord neededEpochRecord = null;
            boolean epochExists = true;
            HistoryTimeSeriesRecord neededHistoryRecord = null;
            boolean historyExists = true;

            // To obtain the corresponding EpochRecord and check if it is corrupted or not
            try {
                neededEpochRecord = store.getEpoch(scope, streamName, transitionRecord.getNewEpoch(),
                        null, executor).join();

            } catch (StoreException.DataNotFoundException e) {
                responseBuilder.append("The corresponding EpochRecord is corrupted or does not exist.").append("\n");
                epochExists = false;
            }

            // To obtain the corresponding HistoryTimeSeriesRecord and check if it corrupted or not
            try {
                neededHistoryRecord = store.getHistoryTimeSeriesRecord(scope, streamName, transitionRecord.getNewEpoch(),
                        null, executor).join();

            } catch (StoreException.DataNotFoundException e) {
                responseBuilder.append("The corresponding HistoryTimeSeriesRecord is corrupted or does not exist.").append("\n");
                historyExists = false;
            }

            // Output the existing records in case of corruption
            if (!(epochExists && historyExists)) {
                responseBuilder.append("EpochTransitionRecord : ");
                responseBuilder.append(outputTransition(transitionRecord));

                if (historyExists) {
                    responseBuilder.append("HistoryTimeSeriesRecord : ");
                    responseBuilder.append(outputHistoryRecord(neededHistoryRecord));
                }

                if (epochExists) {
                    responseBuilder.append("EpochRecord : ");
                    responseBuilder.append(outputEpoch(neededEpochRecord));
                }

                output(responseBuilder.toString());
                return false;
            }

            // Check the EpochRecord and HistoryTimeSeriesRecord
            boolean isConsistent = checkConsistency(neededEpochRecord, neededHistoryRecord);

            if (!isConsistent) {
                responseBuilder.append("Inconsistency among the EpochRecord and the HistoryTimeSeriesRecord").append("\n");
            }

            // Check the HistoryTimeSeriesRecord's sealed segments and make sure they are sealed
            List<Long> sealedSegmentsHistory = neededHistoryRecord.getSegmentsSealed().stream()
                    .map(StreamSegmentRecord::getSegmentNumber)
                    .mapToLong(Integer::longValue)
                    .boxed()
                    .collect(Collectors.toList());

            for (Long id : sealedSegmentsHistory) {
                boolean isSealed = store.checkSegmentSealed(scope, streamName, id, null, executor).join();
                if (!isSealed) {
                    responseBuilder.append("Inconsistency among the HistoryTimeSeriesRecord and the SealedSegmentRecords").append("\n");
                    isConsistent = false;
                    break;
                }
            }

            // Check the EpochTransitionRecord with the EpochRecord and the HistoryTimeSeriesRecord
            // Cross check the segments
            Set<Long> segmentIds = neededEpochRecord.getSegmentIds();
            ImmutableMap<Long, Map.Entry<Double, Double>> newSegments = transitionRecord.getNewSegmentsWithRange();

            for (Long id : segmentIds) {
                SimpleEntry<Double, Double> segmentRange = new SimpleEntry<>(neededEpochRecord.getSegment(id).getKeyStart(),
                        neededEpochRecord.getSegment(id).getKeyEnd());

                if (!segmentRange.equals(newSegments.get(id))) {
                    responseBuilder.append("Inconsistency among the EpochRecord and the EpochTransitionRecord").append("\n");
                    isConsistent = false;
                    break;
                }
            }

            // Cross check the sealed segments
            List<Long> sealedSegmentTransition = new ArrayList<>(transitionRecord.getSegmentsToSeal());

            if (!sealedSegmentTransition.equals(sealedSegmentsHistory)) {
                responseBuilder.append("Inconsistency among the HistoryTimeSeriesRecord and the EpochTransitionRecord").append("\n");
                isConsistent = false;
            }

            // Based on consistency, return all records or none
            if (!isConsistent) {
                responseBuilder.append("EpochTransitionRecord : ").append(outputTransition(transitionRecord));
                responseBuilder.append("EpochRecord : ").append(outputEpoch(neededEpochRecord));
                responseBuilder.append("HistoryTimeSeriesRecord : ").append(outputHistoryRecord(neededHistoryRecord));

                output(responseBuilder.toString());
                return false;
            }

            output("Consistent with respect to scaling");
            return true;

        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
            return true;
        }
    }
}
