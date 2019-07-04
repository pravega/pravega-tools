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

import com.google.common.collect.ImmutableList;
import io.pravega.common.Exceptions;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamStoreFactoryExtended;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
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
 * A helper class that checks the stream with respect to the committing_txn case.
 */
public class CommittingTransactionsCheckCommand extends TroubleshootCommand implements Check{

    protected ExtendedStreamMetadataStore store;

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public CommittingTransactionsCheckCommand(CommandArgs args) { super(args); }

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
                putAllInFaultMap(faults, checkConsistency(duplicateTxnEpochRecord, duplicateTxnHistoryRecord, scope, streamName, store, executor));

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
                putAllInFaultMap(faults, checkConsistency(duplicateActiveEpochRecord, duplicateActiveHistoryRecord, scope, streamName, store, executor));

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
                new ArgDescriptor("stream-name", "Name of the stream"));
    }

    private void checkEmptySegments(final Map<Record, Set<Fault>> faults, final HistoryTimeSeriesRecord record,
                                    final Record historyRecord, final String className) {
        checkEmptySegmentsHelper(faults, record, historyRecord, HistoryTimeSeriesRecord::getSegmentsCreated,
                "segments created", className);

        checkEmptySegmentsHelper(faults, record, historyRecord, HistoryTimeSeriesRecord::getSegmentsSealed,
                "segments sealed", className);
    }

    private void checkEmptySegmentsHelper(final Map<Record, Set<Fault>> faults, final HistoryTimeSeriesRecord record, final Record historyRecord,
                                    final Function<HistoryTimeSeriesRecord, Object> historyFunc, final String field, final String className) {
        boolean segmentsExists = checkCorrupted(record, historyFunc, field, className, faults);

        if (segmentsExists && !((ImmutableList<StreamSegmentRecord>)historyFunc.apply(record)).isEmpty()) {
            putInFaultMap(faults, historyRecord,
                    Fault.inconsistent(historyRecord, className + ": " + field + "are not empty."));
        }
    }

    private void checkOriginal(final Map<Record, Set<Fault>> faults, final int epoch, final EpochRecord duplicateEpochRecord,
                               final String scope, final String streamName, final ExtendedStreamMetadataStore store,
                               final ScheduledExecutorService executor, final String epochType) {
        String originalName = "Original " + epochType + " EpochRecord";
        String duplicateName = "Duplicate " + epochType + " EpochRecord";

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
            List<Long> dup2Segments = originalEpochRecord.getSegmentIds().stream()
                    .map(id -> computeSegmentId(Math.toIntExact(id), originalEpochRecord.getEpoch()))
                    .collect(Collectors.toList());
            List<Long> duplicateSegments = new ArrayList<>(duplicateEpochRecord.getSegmentIds());

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
