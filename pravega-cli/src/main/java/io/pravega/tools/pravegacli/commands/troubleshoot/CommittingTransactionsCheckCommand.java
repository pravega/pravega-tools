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
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.*;
import io.pravega.tools.pravegacli.commands.CommandArgs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.checkConsistency;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.checkCorrupted;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putAllInFaultMap;
import static io.pravega.tools.pravegacli.commands.utils.CheckUtils.putInFaultMap;

/**
 * A helper class that checks the stream with respect to the committing_txn case.
 */
public class CommittingTransactionsCheckCommand extends TroubleshootCommand implements Check{

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public CommittingTransactionsCheckCommand(CommandArgs args) { super(args); }

    @Override
    public void execute() {

    }

    @Override
    public Map<Record, Set<Fault>> check(StreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, Set<Fault>> faults = new HashMap<>();

        // Check for the existence of an CommittingTransactionsRecord.
        CommittingTransactionsRecord committingRecord;

        EpochRecord duplicateTxnEpochRecord = null;
        HistoryTimeSeriesRecord duplicateTxnHistoryRecord = null;

        EpochRecord duplicateActiveEpochRecord = null;
        HistoryTimeSeriesRecord duplicateActiveHistoryRecord = null;


        // To obtain the CommittingTransactionRecord and check if it is corrupted or not.
        try {
            committingRecord = store.getVersionedCommittingTransactionsRecord(scope, streamName, null, executor)
                    .thenApply(VersionedMetadata::getObject).join();

        } catch (StoreException.DataNotFoundException e) {
            Record<CommittingTransactionsRecord> committingTransactionsRecord = new Record<>(null, CommittingTransactionsRecord.class);
            putInFaultMap(faults, committingTransactionsRecord,
                    Fault.unavailable("CommittingTransactionsRecord is corrupted or unavailable"));

            return faults;
        }

        // Check if its a rolling transaction.
        boolean rollingTxnExists = checkCorrupted(committingRecord, CommittingTransactionsRecord::isRollingTxnRecord,
                "active epoch", "CommittingTransactionRecord", faults);

        if (rollingTxnExists) {
            if (committingRecord.isRollingTxnRecord()) {
                boolean epochExists = true;
                boolean historyExists = true;

                // For duplicate transaction epoch.
                try {
                    duplicateTxnEpochRecord = store.getEpoch(scope, streamName, committingRecord.getNewTxnEpoch(),
                            null, executor).join();

                } catch (StoreException.DataNotFoundException e) {
                    Record<EpochRecord> duplicateTxnRecord = new Record<>(null, EpochRecord.class);
                    putInFaultMap(faults, duplicateTxnRecord,
                            Fault.unavailable("Duplicate txn epoch: " + committingRecord.getNewTxnEpoch() + "is corrupted or does not exist."));

                    epochExists = false;
                }

                try {
                    int chunkNumber=committingRecord.getNewTxnEpoch()/HistoryTimeSeries.HISTORY_CHUNK_SIZE;
                    duplicateTxnHistoryRecord = store.getHistoryTimeSeriesChunk(scope, streamName,
                            chunkNumber, null, executor).join().getLatestRecord();

                } catch (StoreException.DataNotFoundException e) {
                    Record<HistoryTimeSeriesRecord> duplicateTxnHistory = new Record<>(null, HistoryTimeSeriesRecord.class);
                    putInFaultMap(faults, duplicateTxnHistory,
                            Fault.unavailable("Duplicate txn history: " + committingRecord.getNewTxnEpoch() + "is corrupted or does not exist."));

                    historyExists = false;
                }

                // Return the faults in case of corruption.
                if (!(epochExists && historyExists)) {
                    return faults;
                }

                // Check consistency for EpochRecord and HistoryTimeSeriesRecord for duplicate txn.
                putAllInFaultMap(faults, checkConsistency(duplicateTxnEpochRecord, duplicateTxnHistoryRecord, scope, streamName, store, executor));

                Record<EpochRecord> duplicateTxnRecord = new Record<>(duplicateTxnEpochRecord, EpochRecord.class);
                Record<HistoryTimeSeriesRecord> duplicateTxnHistory = new Record<>(duplicateTxnHistoryRecord, HistoryTimeSeriesRecord.class);

                epochExists = true;
                historyExists = true;

                // For duplicate active epoch record.
                try {
                    duplicateActiveEpochRecord = store.getEpoch(scope, streamName, committingRecord.getNewActiveEpoch(),
                            null, executor).join();

                } catch (StoreException.DataNotFoundException e) {
                    Record<EpochRecord> duplicateActiveRecord = new Record<>(null, EpochRecord.class);
                    putInFaultMap(faults, duplicateActiveRecord,
                            Fault.unavailable("Duplicate active epoch: " + committingRecord.getNewTxnEpoch() + "is corrupted or does not exist."));

                    epochExists = false;
                }

                try {
                    int chunkNumber=committingRecord.getNewTxnEpoch()/ HistoryTimeSeries.HISTORY_CHUNK_SIZE;
                    duplicateActiveHistoryRecord = store.getHistoryTimeSeriesChunk(scope, streamName,
                            chunkNumber, null, executor).join().getLatestRecord();

                } catch (StoreException.DataNotFoundException e) {
                    Record<HistoryTimeSeriesRecord> duplicateActiveHistory = new Record<>(null, HistoryTimeSeriesRecord.class);
                    putInFaultMap(faults, duplicateActiveHistory,
                            Fault.unavailable("Duplicate active history: " + committingRecord.getNewTxnEpoch() + "is corrupted or does not exist."));

                    historyExists = false;
                }

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
                               final String scope, final String streamName, final StreamMetadataStore store,
                               final ScheduledExecutorService executor, final String epochType) {
        EpochRecord originalEpochRecord;

        String originalName = "Original " + epochType + " EpochRecord";
        String duplicateName = "Duplicate " + epochType + " EpochRecord";

        try {
            originalEpochRecord = store.getEpoch(scope, streamName, epoch,
                    null, executor).join();

        } catch (StoreException.DataNotFoundException e) {
            Record<EpochRecord> originalRecord = new Record<>(null, EpochRecord.class);
            putInFaultMap(faults, originalRecord,
                    Fault.unavailable(originalName + ": " + epoch + " is corrupted or unavailable."));

            return;
        }

        Record<EpochRecord> originalRecord = new Record<>(originalEpochRecord, EpochRecord.class);
        Record<EpochRecord> duplicateRecord = new Record<>(duplicateEpochRecord, EpochRecord.class);

        // Check the duplicate segments.
        EpochRecord finalTxnEpochRecord = originalEpochRecord;

        boolean originalSegmentExists = checkCorrupted(originalEpochRecord, EpochRecord::getSegmentIds,
                "segments", originalName, faults);
        boolean duplicateSegmentExists =  checkCorrupted(duplicateEpochRecord, EpochRecord::getSegmentIds,
                "segments", duplicateName, faults);

        if (originalSegmentExists && duplicateSegmentExists) {
            List<Long> dup2Segments = originalEpochRecord.getSegmentIds().stream()
                    .map(id -> computeSegmentId(Math.toIntExact(id), finalTxnEpochRecord.getEpoch()))
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
                && duplicateEpochRecord.getReferenceEpoch() != finalTxnEpochRecord.getReferenceEpoch()) {
            putInFaultMap(faults, duplicateRecord,
                    Fault.inconsistent(originalRecord, "Fault in the reference epochs"));
        }
    }
}
