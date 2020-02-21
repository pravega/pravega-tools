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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.pravega.common.Exceptions;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.*;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.pravega.shared.NameUtils.computeSegmentId;
import static io.pravega.shared.NameUtils.getEpoch;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.*;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputFaults;

/**
 * A helper class that checks the stream with respect to the committing_txn case.
 */
public class CommittingTransactionsCheckCommand extends TroubleshootCommandHelper implements Check{

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public CommittingTransactionsCheckCommand(CommandArgs args) { super(args); }

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
            output(outputFaults(faults));

        } catch (CompletionException e) {
            System.err.println("Exception during process: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Exception accessing metadata store: " + e.getMessage());
        }
    }

    /**
     * Method to check the consistency of the stream with respect to the committing transactions workflow. We first obtain the
     * CommittingTransactionsRecord and then run the following checks:
     *
     * - If its not rolling then there are no more checks
     *
     * - If it is rolling then, we get the duplicate transaction epoch and the duplicate active record that are generated
     *   in the committing process. The corresponding history records are also obtained. If these are corrupted we return.
     *
     * - Check for any faults between each pair of epoch and history records.
     *
     * - Check for the emptiness of the segment lists in the duplicate history records.
     *
     * - Check whether the times of the duplicates is correct. The duplicate txn epoch should be followed by the duplicate active
     *   epoch. Creation time of duplicate txn < Creation time of duplicate active.
     *
     * - Make sure that the duplicate records are in line with the original epoch record from which they are derived.
     *
     * Any faults which are noticed are immediately recorded and then finally returned.
     *
     * @param store     an instance of the stream metadata store
     * @param executor  callers executor
     * @return A map of Record and a set of Faults associated with it.
     */
    @Override
    public Map<Record, Set<Fault>> check(StreamMetadataStore store, ScheduledExecutorService executor) {
        checkTroubleshootArgs();
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, Set<Fault>> faults = new HashMap<>();

        // Check for the existence of an CommittingTransactionsRecord.
        CommittingTransactionsRecord committingRecord = store.getVersionedCommittingTransactionsRecord(scope, streamName, null, executor)
                .handle((x, e) -> {
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                            Record<CommittingTransactionsRecord> committingTransactionsRecord = new Record<>(null, CommittingTransactionsRecord.class);
                            putInFaultMap(faults, committingTransactionsRecord,
                                    Fault.unavailable("CommittingTransactionsRecord is corrupted or unavailable"));
                            return null;
                        } else {
                            throw new CompletionException(e);
                        }
                    }
                    return x.getObject();
                }).join();

        if (committingRecord == null) {
            return faults;
        }

        // If the CommittingTransactionsRecord is EMPTY then there's no need to check further.
        if (committingRecord.equals(CommittingTransactionsRecord.EMPTY)) {
            return faults;
        }

        // Check if its a rolling transaction.
        boolean rollingTxnExists = checkCorrupted(committingRecord, CommittingTransactionsRecord::isRollingTxnRecord,
                "active epoch", "CommittingTransactionsRecord", faults);

        if (rollingTxnExists) {
            if (committingRecord.isRollingTxnRecord()) {
                boolean epochExists;
                boolean historyExists;

                // For duplicate transaction epoch.
                EpochRecord duplicateTxnEpochRecord = getEpochIfExists(store, executor, scope, streamName, committingRecord.getNewTxnEpoch(), faults);
                epochExists = duplicateTxnEpochRecord != null;

                HistoryTimeSeriesRecord duplicateTxnHistoryRecord = getHistoryTimeSeriesRecordIfExists(store, executor, scope, streamName, committingRecord.getNewTxnEpoch(), faults);
                historyExists = duplicateTxnHistoryRecord != null;

                // Return the faults in case of corruption.
                if (!(epochExists && historyExists)) {
                    return faults;
                }

                // Check consistency for EpochRecord and HistoryTimeSeriesRecord for duplicate txn.
                putAllInFaultMap(faults, checkConsistency(duplicateTxnEpochRecord, duplicateTxnHistoryRecord, true, scope, streamName, store, executor));

                Record<EpochRecord> duplicateTxnRecord = new Record<>(duplicateTxnEpochRecord, EpochRecord.class);
                Record<HistoryTimeSeriesRecord> duplicateTxnHistory = new Record<>(duplicateTxnHistoryRecord, HistoryTimeSeriesRecord.class);

                // For duplicate active epoch record.
                EpochRecord duplicateActiveEpochRecord = getEpochIfExists(store, executor, scope, streamName, committingRecord.getNewActiveEpoch(), faults);
                epochExists = duplicateActiveEpochRecord != null;

                HistoryTimeSeriesRecord duplicateActiveHistoryRecord = getHistoryTimeSeriesRecordIfExists(store, executor, scope, streamName, committingRecord.getNewActiveEpoch(), faults);
                historyExists = duplicateActiveHistoryRecord != null;

                // Return the faults in case of corruption.
                if (!(epochExists && historyExists)) {
                    return faults;
                }

                // Check consistency for EpochRecord and HistoryTimeSeriesRecord for duplicate active.
                putAllInFaultMap(faults, checkConsistency(duplicateActiveEpochRecord, duplicateActiveHistoryRecord, true, scope, streamName, store, executor));

                Record<EpochRecord> duplicateActiveRecord = new Record<>(duplicateActiveEpochRecord, EpochRecord.class);
                Record<HistoryTimeSeriesRecord> duplicateActiveHistory = new Record<>(duplicateActiveHistoryRecord, HistoryTimeSeriesRecord.class);

                // Check the emptiness of the HistoryTimeSeriesRecords.
                checkEmptySegments(faults, duplicateTxnHistoryRecord, duplicateTxnHistory, "Duplicate txn history");
                checkEmptySegments(faults, duplicateActiveHistoryRecord, duplicateActiveHistory, "Duplicate active history");

                // Check the time for the EpochRecords.
                boolean activeCreationTimeExists = checkCorrupted(duplicateActiveEpochRecord, EpochRecord::getCreationTime,
                        "creation time", "duplicate active epoch", faults);

                boolean txnCreationTimeExists = checkCorrupted(duplicateTxnEpochRecord, EpochRecord::getCreationTime,
                        "creation time", "duplicate txn epoch", faults);

                if (activeCreationTimeExists && txnCreationTimeExists) {
                    if (duplicateActiveEpochRecord.getCreationTime() != duplicateTxnEpochRecord.getCreationTime() + 1) {
                        putInFaultMap(faults, duplicateTxnRecord,
                                Fault.inconsistent(duplicateActiveRecord, "Fault: duplicates time's are not ordered properly."));
                    }
                }

                checkOriginal(faults, committingRecord.getEpoch(), duplicateTxnEpochRecord, scope, streamName,
                        store, executor, "Txn");
                checkOriginal(faults, committingRecord.getCurrentEpoch(), duplicateActiveEpochRecord, scope, streamName,
                        store, executor, "Active");
            }
        }

        return faults;
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "committing_txn-check", "check health of the committing_txn workflow",
                new ArgDescriptor("scope-name", "Name of the scope"),
                new ArgDescriptor("stream-name", "Name of the stream"),
                new ArgDescriptor("output-file", "(OPTIONAL) The file to output the results to"));
    }

    /**
     * A method that checks if the duplicate HistoryTimeSeriesRecord's segments created list and segments sealed list are
     * empty. If this invariant is not followed, it adds the corresponding faults into the fault map provided.
     *
     * @param faults        the fault map to add to
     * @param record        the HistoryTimeSeriesRecord
     * @param historyRecord the wrapped version of the history record for creating the map
     * @param className     the name of the record when recording the error message in the fault
     */
    private void checkEmptySegments(final Map<Record, Set<Fault>> faults, final HistoryTimeSeriesRecord record,
                                    final Record historyRecord, final String className) {
        checkEmptySegmentsHelper(faults, record, historyRecord, HistoryTimeSeriesRecord::getSegmentsCreated,
                "segments created", className);

        checkEmptySegmentsHelper(faults, record, historyRecord, HistoryTimeSeriesRecord::getSegmentsSealed,
                "segments sealed", className);
    }

    /**
     * A helper method that checks if the HistoryTimeSeriesRecord and its given segments list is empty or not. It takes the
     * getter method for the segment list in question and checks if its corrupted. If it isn't then we check if the list is
     * empty or not and then add the corresponding faults into the fault map.
     *
     * @param faults        the fault map to add to
     * @param record        the HistoryTimeSeriesRecord
     * @param historyRecord the wrapped version of the history record for creating the map
     * @param historyFunc   the getter method reference for the segment list
     * @param field         the name of the field when recording the error message in the fault
     * @param className     the name of the record when recording the error message in the fault
     */
    private void checkEmptySegmentsHelper(final Map<Record, Set<Fault>> faults, final HistoryTimeSeriesRecord record, final Record historyRecord,
                                          final Function<HistoryTimeSeriesRecord, Object> historyFunc, final String field, final String className) {
        boolean segmentsExists = checkCorrupted(record, historyFunc, field, className, faults);

        if (segmentsExists && !((ImmutableList<StreamSegmentRecord>)historyFunc.apply(record)).isEmpty()) {
            putInFaultMap(faults, historyRecord,
                    Fault.inconsistent(historyRecord, className + ": " + field + " are not empty."));
        }
    }

    /**
     * A method to obtain the original epoch record and then compare it with the duplicate epoch record provided. We
     * safely obtain the original epoch record. We then check the following:
     *
     * - The first check is to see if the segment numbers generated in the duplicate epoch are computed correctly using the
     *   original epoch and the original epoch's segment numbers.
     *
     * - The second check is to see if the reference epochs of the original and the duplicate epochs are the same.
     *
     * In case of any of the above checks failing the corresponding fault is registered into the fault map.
     *
     * @param faults               the fault map to add to
     * @param epoch                the original epoch
     * @param duplicateEpochRecord the duplicate epoch record
     * @param scope                scope name
     * @param streamName           stream name
     * @param store                an instance of the stream metadata store
     * @param executor             callers executor
     * @param epochType            a string indicating the type of epoch,
     *                             to be used when registering faults only
     */
    private void checkOriginal(final Map<Record, Set<Fault>> faults, final int epoch, final EpochRecord duplicateEpochRecord,
                               final String scope, final String streamName, final StreamMetadataStore store,
                               final ScheduledExecutorService executor, final String epochType) {
        String originalName = "Original " + epochType + " EpochRecord";
        String duplicateName = "Duplicate " + epochType + " EpochRecord";

        // Obtain the original record.
        EpochRecord originalEpochRecord = getEpochIfExists(store, executor, scope, streamName, epoch, faults);
        if (originalEpochRecord == null) {
            return;
        }

        Record<EpochRecord> originalRecord = new Record<>(originalEpochRecord, EpochRecord.class);
        Record<EpochRecord> duplicateRecord = new Record<>(duplicateEpochRecord, EpochRecord.class);

        // Check the duplicate segments.
        boolean originalSegmentExists = checkCorrupted(originalEpochRecord, EpochRecord::getSegmentIds,
                "segments", originalName, faults);
        boolean duplicateSegmentExists =  checkCorrupted(duplicateEpochRecord, EpochRecord::getSegmentIds,
                "segments", duplicateName, faults);

        if (originalSegmentExists && duplicateSegmentExists) {
            List<Integer> dup2Segments = originalEpochRecord.getSegments().stream()
                    .map(segment -> getEpoch(computeSegmentId(Math.toIntExact(segment.getSegmentNumber()), originalEpochRecord.getEpoch())))
                    .collect(Collectors.toList());

            List<Integer> duplicateSegments = duplicateEpochRecord.getSegments().stream()
                    .map(StreamSegmentRecord::getSegmentNumber)
                    .collect(Collectors.toList());

            if (!duplicateSegments.equals(dup2Segments)) {
                putInFaultMap(faults, duplicateRecord,
                        Fault.inconsistent(originalRecord, "Faults in the generated duplicate segments"));
            }
        }

        // Check the reference epochs.
        boolean originalReferenceExists = checkCorrupted(originalEpochRecord, EpochRecord::getReferenceEpoch,
                "reference epoch", originalName, faults);
        boolean duplicateReferenceExists = checkCorrupted(duplicateEpochRecord, EpochRecord::getReferenceEpoch,
                "reference epoch", duplicateName, faults);

        if (originalReferenceExists && duplicateReferenceExists
                && (duplicateEpochRecord.getReferenceEpoch() != originalEpochRecord.getReferenceEpoch())) {
            putInFaultMap(faults, duplicateRecord,
                    Fault.inconsistent(originalRecord, "Fault in the reference epochs"));
        }
    }
}
