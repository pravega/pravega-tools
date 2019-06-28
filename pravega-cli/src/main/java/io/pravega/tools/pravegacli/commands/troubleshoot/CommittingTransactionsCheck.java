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
import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class CommittingTransactionsCheck extends TroubleshootCommand implements Check{

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public CommittingTransactionsCheck(CommandArgs args) { super(args); }

    @Override
    public void execute() {

    }

    @Override
    public Map<Record, List<Fault>> check(ExtendedStreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        Map<Record, List<Fault>> faults = new HashMap<>();

        // Check for the existence of an CommittingTransactionsRecord.
        CommittingTransactionsRecord committingRecord;

        EpochRecord txnEpochRecord = null;
        EpochRecord duplicateTxnEpochRecord = null;
        HistoryTimeSeriesRecord duplicateTxnHistoryRecord = null;

        EpochRecord activeEpochRecord = null;
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

        Record<CommittingTransactionsRecord> committingTransactionsRecord = new Record<>(committingRecord, CommittingTransactionsRecord.class);

        // Check if its a rolling transaction.
        Fault rollingTxnFault = checkCorrupted(committingRecord, CommittingTransactionsRecord::isRollingTxnRecord,
                "active epoch", "CommittingTransactionRecord");

        if (rollingTxnFault != null) {
            putInFaultMap(faults, committingTransactionsRecord, rollingTxnFault);
        } else {

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
                    duplicateTxnHistoryRecord = store.getHistoryTimeSeriesRecord(scope, streamName,
                            committingRecord.getNewTxnEpoch(), null, executor).join();

                } catch (StoreException.DataNotFoundException e) {
                    Record<HistoryTimeSeriesRecord> duplicateTxnHistory = new Record<>(null, HistoryTimeSeriesRecord.class);
                    putInFaultMap(faults, duplicateTxnHistory,
                            Fault.unavailable("Duplicate txn history: "+ committingRecord.getNewTxnEpoch() + "is corrupted or does not exist."));

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
                    duplicateActiveHistoryRecord = store.getHistoryTimeSeriesRecord(scope, streamName,
                            committingRecord.getNewActiveEpoch(), null, executor).join();

                } catch (StoreException.DataNotFoundException e) {
                    Record<HistoryTimeSeriesRecord> duplicateActiveHistory = new Record<>(null, HistoryTimeSeriesRecord.class);
                    putInFaultMap(faults, duplicateActiveHistory,
                            Fault.unavailable("Duplicate active history: "+ committingRecord.getNewTxnEpoch() + "is corrupted or does not exist."));

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
                checkEmptySegments(faults, duplicateTxnHistoryRecord, duplicateTxnHistory, HistoryTimeSeriesRecord::getSegmentsCreated,
                        "segments created", "duplicate txn history");

                checkEmptySegments(faults, duplicateTxnHistoryRecord, duplicateTxnHistory, HistoryTimeSeriesRecord::getSegmentsSealed,
                        "segments sealed", "Duplicate txn history");

                checkEmptySegments(faults, duplicateActiveHistoryRecord, duplicateActiveHistory, HistoryTimeSeriesRecord::getSegmentsCreated,
                        "segments created", "duplicate active history");

                checkEmptySegments(faults, duplicateActiveHistoryRecord, duplicateActiveHistory, HistoryTimeSeriesRecord::getSegmentsSealed,
                        "segments sealed", "Duplicate active history");

                // Check the time for the EpochRecords.
                Fault activeCreationTimeFault = checkCorrupted(duplicateActiveEpochRecord, EpochRecord::getCreationTime,
                        "creation time", "duplicate active epoch");

                Fault txnCreationTimeFault = checkCorrupted(duplicateTxnEpochRecord, EpochRecord::getCreationTime,
                        "creation time", "duplicate tx epoch");

                if (activeCreationTimeFault != null) {
                    putInFaultMap(faults, duplicateActiveRecord, activeCreationTimeFault);
                }

                if (txnCreationTimeFault != null) {
                    putInFaultMap(faults, duplicateTxnRecord, txnCreationTimeFault);
                }

                if (activeCreationTimeFault == null && txnCreationTimeFault == null) {
                    if (duplicateActiveEpochRecord.getCreationTime() != duplicateTxnEpochRecord.getCreationTime() + 1) {
                        putInFaultMap(faults, duplicateTxnRecord,
                                Fault.inconsistent(duplicateActiveRecord, "Fault: duplicates time's are not ordered properly."));
                    }
                }

                // Get the original records.
                boolean txnExists = true;
                try {
                    txnEpochRecord = store.getEpoch(scope, streamName, committingRecord.getEpoch(),
                            null, executor).join();

                } catch (StoreException.DataNotFoundException e) {
                    responseBuilder.append("Original txnEpochRecord is corrupted or unavailable.").append("\n");
                    txnExists = false;
                }

                if (txnExists) {
                    // Check the duplicate segments.
                    EpochRecord finalTxnEpochRecord = txnEpochRecord;
                    List<Long> dup2Segments = txnEpochRecord.getSegmentIds().stream()
                            .map(id -> computeSegmentId(Math.toIntExact(id), finalTxnEpochRecord.getEpoch()))
                            .collect(Collectors.toList());
                    List<Long> duplicateSegments = new ArrayList<>(duplicateTxnEpochRecord.getSegmentIds());

                    if (!duplicateSegments.equals(dup2Segments)) {
                        responseBuilder.append("Duplicate txn: Fault in the generated duplicate segments").append("\n");
                    }

                    // Check the reference epochs.
                    if (duplicateActiveEpochRecord.getReferenceEpoch() != finalTxnEpochRecord.getReferenceEpoch()) {
                        responseBuilder.append("Duplicate txn: Fault in the reference epochs").append("\n");
                    }
                }


                boolean activeExists = true;
                try {
                    activeEpochRecord = store.getEpoch(scope, streamName, committingRecord.getCurrentEpoch(),
                            null, executor).join();

                } catch (StoreException.DataNotFoundException e) {
                    responseBuilder.append("Original activeEpochRecord is corrupted or unavailable.").append("\n");
                    activeExists = false;
                }

                if (activeExists) {
                    // Check the duplicate segments.
                    EpochRecord finalActiveEpochRecord = activeEpochRecord;
                    List<Long> dup2Segments = activeEpochRecord.getSegmentIds().stream()
                            .map(id -> computeSegmentId(Math.toIntExact(id), finalActiveEpochRecord.getEpoch()))
                            .collect(Collectors.toList());
                    List<Long> duplicateSegments = new ArrayList<>(duplicateActiveEpochRecord.getSegmentIds());

                    if (!duplicateSegments.equals(dup2Segments)) {
                        responseBuilder.append("Duplicate active : Fault in the generated duplicate segments").append("\n");
                    }

                    // Check the reference epochs.
                    if (duplicateActiveEpochRecord.getReferenceEpoch() != finalActiveEpochRecord.getReferenceEpoch()) {
                        responseBuilder.append("Duplicate active: Fault in the reference epochs").append("\n");
                    }
                }

            }
        }

        return faults;
    }

    private void checkEmptySegments(final Map<Record, List<Fault>> faults, final HistoryTimeSeriesRecord record, final Record historyRecord,
                                    final Function<HistoryTimeSeriesRecord, Object> historyFunc, final String field, final String className) {
        Fault segmentsFault = checkCorrupted(record, historyFunc, field, className);

        if (segmentsFault != null) {
            putInFaultMap(faults, historyRecord, segmentsFault);
        }

        if (!((ImmutableList<StreamSegmentRecord>)historyFunc.apply(record)).isEmpty()) {
            putInFaultMap(faults, historyRecord,
                    Fault.inconsistent(historyRecord, className + ": " + field + "are not empty."));
        }
    }

    private void checkOriginal(final Map<Record, List<Fault>> faults, final int epoch, final EpochRecord duplicateTxnEpochRecord,
                               final String scope, final String streamName, final ExtendedStreamMetadataStore store,
                               final ScheduledExecutorService executor) {
        boolean epochExists = true;
        EpochRecord originalEpochRecord = null;

        try {
            originalEpochRecord = store.getEpoch(scope, streamName, epoch,
                    null, executor).join();

        } catch (StoreException.DataNotFoundException e) {
            responseBuilder.append("Original txnEpochRecord is corrupted or unavailable.").append("\n");
            epochExists = false;
        }

        if (epochExists) {
            // Check the duplicate segments.
            EpochRecord finalTxnEpochRecord = originalEpochRecord;
            List<Long> dup2Segments = originalEpochRecord.getSegmentIds().stream()
                    .map(id -> computeSegmentId(Math.toIntExact(id), finalTxnEpochRecord.getEpoch()))
                    .collect(Collectors.toList());
            List<Long> duplicateSegments = new ArrayList<>(duplicateTxnEpochRecord.getSegmentIds());

            if (!duplicateSegments.equals(dup2Segments)) {
                responseBuilder.append("Duplicate txn: Fault in the generated duplicate segments").append("\n");
            }

            // Check the reference epochs.
            if (duplicateTxnEpochRecord.getReferenceEpoch() != finalTxnEpochRecord.getReferenceEpoch()) {
                responseBuilder.append("Duplicate txn: Fault in the reference epochs").append("\n");
            }
        }
    }
}
