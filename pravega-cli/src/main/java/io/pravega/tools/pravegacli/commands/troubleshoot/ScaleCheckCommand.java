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
import io.pravega.common.Exceptions;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamStoreFactoryExtended;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.checkConsistency;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.checkCorrupted;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.getEpochIfExists;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.getHistoryTimeSeriesRecordIfExists;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putAllInFaultMap;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putInFaultMap;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputFaults;

/**
 * A command that checks the stream with respect to the scale case.
 */
public class ScaleCheckCommand extends TroubleshootCommand implements Check {

    protected ExtendedStreamMetadataStore store;

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ScaleCheckCommand(CommandArgs args) { super(args); }

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
                store = StreamStoreFactoryExtended.createZKStore(zkClient, executor);
            } else {
                segmentHelper = instantiateSegmentHelper(zkClient);
                AuthHelper authHelper = AuthHelper.getDisabledAuthHelper();
                store = StreamStoreFactoryExtended.createPravegaTablesStore(segmentHelper, authHelper, zkClient, executor);
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
     * Method to check the consistency of the stream with respect to scaling workflow. We first obtain the EpochTransitionRecord
     * and then run the following checks:
     *
     * - If the EpochTransitionRecord is not empty then we try to obtain the new epoch record as dictated by the EpochTransitionRecord.
     *
     * - Once we have the epoch record, we also obtain the corresponding history record and then run the desired consistency
     *   checks among them. If any one of the record is not available then we stop and return all the faults upto this point.
     *
     * - We check whether the segments created list in the epoch record is in line with the segments described by the new
     *   ranges in the EpochTransitionRecord.
     *
     * - We check if the segments sealed as mentioned in the HistoryTimeSeriesRecord are equivalent to the segments to be sealed
     *   as described in the EpochTransitionRecord.
     *
     * Any faults which are noticed are immediately recorded and then finally returned.
     *
     * @param store     an instance of the extended metadata store
     * @param executor  callers executor
     * @return A map of Record and a set of Faults associated with it.
     */
    @Override
    public Map<Record, Set<Fault>> check(ExtendedStreamMetadataStore store, ScheduledExecutorService executor) {
        checkTroubleshootArgs();
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, Set<Fault>> faults = new HashMap<>();

        // Check for the existence of an EpochTransitionRecord.
        EpochTransitionRecord transitionRecord = store.getEpochTransition(scope, streamName, null, executor)
                .handle((x, e) -> {
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                            Record<EpochTransitionRecord> epochTransitionRecord = new Record<>(null, EpochTransitionRecord.class);
                            putInFaultMap(faults, epochTransitionRecord,
                                    Fault.unavailable("EpochTransitionRecord is corrupted or unavailable"));
                            return null;
                        } else {
                            throw new CompletionException(e);
                        }
                    }
                    return x.getObject();
                }).join();

        if (transitionRecord == null) {
            return faults;
        }

        // If the EpochTransitionRecord is EMPTY then there's no need to check further.
        if (transitionRecord.equals(EpochTransitionRecord.EMPTY)) {
            return faults;
        }

        if (!checkCorrupted(transitionRecord, EpochTransitionRecord::getNewEpoch, "new epoch", "EpochTransitionRecord", faults)) {
            return faults;
        }

        // To obtain the corresponding EpochRecord and check if it is corrupted or not.
        EpochRecord neededEpochRecord = getEpochIfExists(store, executor, scope, streamName, transitionRecord.getNewEpoch(), faults);
        boolean epochExists = neededEpochRecord != null;

        // To obtain the corresponding HistoryTimeSeriesRecord and check if it corrupted or not.
        HistoryTimeSeriesRecord neededHistoryRecord = getHistoryTimeSeriesRecordIfExists(store, executor, scope, streamName, transitionRecord.getNewEpoch(), faults);
        boolean historyExists = neededHistoryRecord != null;

        // Return the faults in case of corruption.
        if (!(epochExists && historyExists)) {
            return faults;
        }

        // Check the EpochRecord and HistoryTimeSeriesRecord.
        putAllInFaultMap(faults, checkConsistency(neededEpochRecord, neededHistoryRecord, false, scope, streamName, store, executor));

        Record<EpochTransitionRecord> epochTransitionRecord = new Record<>(transitionRecord, EpochTransitionRecord.class);
        Record<EpochRecord> epochRecord = new Record<>(neededEpochRecord, EpochRecord.class);
        Record<HistoryTimeSeriesRecord> historyTimeSeriesRecord = new Record<>(neededHistoryRecord, HistoryTimeSeriesRecord.class);

        // Check the EpochTransitionRecord with the EpochRecord and the HistoryTimeSeriesRecord.
        // Cross check the segments
        boolean getSegmentsExists = checkCorrupted(transitionRecord, EpochTransitionRecord::getNewSegmentsWithRange,
                "segments created", "EpochTransitionRecord", faults);
        if (getSegmentsExists) {
            Set<Long> segmentIds = neededEpochRecord.getSegmentIds();
            ImmutableMap<Long, Map.Entry<Double, Double>> newSegments = transitionRecord.getNewSegmentsWithRange();

            for (Long id : segmentIds) {
                SimpleEntry<Double, Double> segmentRange = new SimpleEntry<>(neededEpochRecord.getSegment(id).getKeyStart(),
                        neededEpochRecord.getSegment(id).getKeyEnd());

                if (!newSegments.containsValue(segmentRange)) {
                    putInFaultMap(faults, epochTransitionRecord,
                            Fault.inconsistent(epochRecord, "EpochRecord and the EpochTransitionRecord mismatch in the segments"));
                    break;
                }
            }
        }

        // Cross check the sealed segments.
        boolean getSealedSegmentsExists = checkCorrupted(transitionRecord, EpochTransitionRecord::getSegmentsToSeal,
                "segments to be sealed", "EpochTransitionRecord", faults);
        if (getSealedSegmentsExists) {
            List<Long> sealedSegmentTransition = new ArrayList<>(transitionRecord.getSegmentsToSeal());

            List<Long> sealedSegmentsHistory = neededHistoryRecord.getSegmentsSealed().stream()
                    .map(s -> computeSegmentId(s.getSegmentNumber(), s.getCreationEpoch()))
                    .collect(Collectors.toList());

            if (!sealedSegmentTransition.equals(sealedSegmentsHistory)) {
                putInFaultMap(faults, epochTransitionRecord,
                        Fault.inconsistent(historyTimeSeriesRecord, "HistoryTimeSeriesRecord and EpochTransitionRecord mismatch in the sealed segments"));
            }
        }

        return faults;
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "scale-check", "check health of the scale workflow",
                new ArgDescriptor("scope-name", "Name of the scope"),
                new ArgDescriptor("stream-name", "Name of the stream"),
                new ArgDescriptor("output-file", "(OPTIONAL) The file to output the results to"));
    }
}
