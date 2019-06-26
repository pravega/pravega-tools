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
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.tools.pravegacli.commands.CommandArgs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static io.pravega.tools.pravegacli.commands.troubleshoot.EpochHistoryCrossCheck.checkConsistency;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.*;

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
    public boolean check(ExtendedStreamMetadataStore store, ScheduledExecutorService executor) {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String streamName = getCommandArgs().getArgs().get(1);
        StringBuilder responseBuilder = new StringBuilder();

        boolean isConsistent;

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
            responseBuilder.append("CommittingTransactionsRecord is corrupted").append("\n");
            output(responseBuilder.toString());
            return false;
        }


        // Check if its a rolling transaction.
        if (committingRecord.isRollingTxnRecord()) {
            responseBuilder.append("The CommittingTransactionsRecord is a rolling transaction record").append("\n");

            boolean epochExists = true;
            boolean historyExists = true;

            // For duplicate transaction epoch.
            try {
                duplicateTxnEpochRecord = store.getEpoch(scope, streamName, committingRecord.getNewTxnEpoch(),
                        null, executor).join();

            } catch (StoreException.DataNotFoundException e) {
                responseBuilder.append("The duplicate active EpochRecord is corrupted or does not exist.").append("\n");
                epochExists = false;
            }

            try {
                duplicateTxnHistoryRecord = store.getHistoryTimeSeriesRecord(scope, streamName,
                        committingRecord.getNewActiveEpoch(), null, executor).join();

            } catch (StoreException.DataNotFoundException e) {
                responseBuilder.append("The corresponding duplicate active history record does not exist.").append("\n");
                historyExists = false;
            }

            // Output the existing records in case of corruption.
            if (!(epochExists && historyExists)) {
                responseBuilder.append("CommittingTransactionsRecord : ");
                responseBuilder.append(outputCommittingTransactions(committingRecord));

                if (historyExists) {
                    responseBuilder.append("Duplicate Active HistoryTimeSeriesRecord : ");
                    responseBuilder.append(outputHistoryRecord(duplicateTxnHistoryRecord));
                }

                if (epochExists) {
                    responseBuilder.append("Duplicate Active EpochRecord : ");
                    responseBuilder.append(outputEpoch(duplicateTxnEpochRecord));
                }

                output(responseBuilder.toString());
                return false;
            }


            epochExists = true;
            historyExists = true;

            // For duplicate active epoch record.
            try {
                duplicateActiveEpochRecord = store.getEpoch(scope, streamName, committingRecord.getNewActiveEpoch(),
                        null, executor).join();

            } catch (StoreException.DataNotFoundException e) {
                responseBuilder.append("The duplicate active EpochRecord is corrupted or does not exist.").append("\n");
                epochExists = false;
            }

            try {
                duplicateActiveHistoryRecord = store.getHistoryTimeSeriesRecord(scope, streamName,
                        committingRecord.getNewActiveEpoch(), null, executor).join();

            } catch (StoreException.DataNotFoundException e) {
                responseBuilder.append("The corresponding duplicate active history record does not exist.").append("\n");
                historyExists = false;
            }

            // Output the existing records in case of corruption.
            if (!(epochExists && historyExists)) {
                responseBuilder.append("CommittingTransactionsRecord : ");
                responseBuilder.append(outputCommittingTransactions(committingRecord));

                if (historyExists) {
                    responseBuilder.append("Duplicate Active HistoryTimeSeriesRecord : ");
                    responseBuilder.append(outputHistoryRecord(duplicateActiveHistoryRecord));
                }

                if (epochExists) {
                    responseBuilder.append("Duplicate Active EpochRecord : ");
                    responseBuilder.append(outputEpoch(duplicateActiveEpochRecord));
                }

                output(responseBuilder.toString());
                return false;
            }


            // Check consistency for EpochRecord and HistoryTimeSeriesRecord for duplicate txn.
            if (!checkConsistency(duplicateTxnEpochRecord, duplicateTxnHistoryRecord, scope, streamName, store, executor)) {
                responseBuilder.append("DuplicateTxn: Inconsistency among the EpochRecord and the HistoryTimeSeriesRecord").append("\n");
            }
            isConsistent = checkConsistency(duplicateTxnEpochRecord, duplicateTxnHistoryRecord, scope, streamName, store, executor);


            // Check consistency for EpochRecord and HistoryTimeSeriesRecord for duplicate active.
            if (!checkConsistency(duplicateActiveEpochRecord, duplicateActiveHistoryRecord, scope, streamName, store, executor)) {
                responseBuilder.append("Duplicate active: Inconsistency among the EpochRecord and the HistoryTimeSeriesRecord").append("\n");
            }
            isConsistent = isConsistent && checkConsistency(duplicateActiveEpochRecord, duplicateActiveHistoryRecord, scope, streamName, store, executor);


            // Check the emptiness of the HistoryTimeSeriesRecords.
            isConsistent = isConsistent &&
                    (duplicateTxnHistoryRecord.getSegmentsCreated().isEmpty() && duplicateTxnHistoryRecord.getSegmentsSealed().isEmpty());

            if (!(duplicateTxnHistoryRecord.getSegmentsCreated().isEmpty() && duplicateTxnHistoryRecord.getSegmentsSealed().isEmpty())) {
                responseBuilder.append("Duplicate txn: The history record segments are not empty.").append("\n");
            }

            isConsistent = isConsistent &&
                    (duplicateActiveHistoryRecord.getSegmentsCreated().isEmpty() && duplicateActiveHistoryRecord.getSegmentsSealed().isEmpty());

            if (!(duplicateActiveHistoryRecord.getSegmentsCreated().isEmpty() && duplicateActiveHistoryRecord.getSegmentsSealed().isEmpty())) {
                responseBuilder.append("Duplicate active: The history record segments are not empty.").append("\n");
            }


            // Check the time for the EpochRecords.
            if (duplicateActiveEpochRecord.getCreationTime() != duplicateTxnEpochRecord.getCreationTime() + 1) {
                responseBuilder.append("Inconsistency: duplicates time's are not ordered properly.").append("\n");
            }
            isConsistent = isConsistent && duplicateActiveEpochRecord.getCreationTime() == duplicateTxnEpochRecord.getCreationTime() + 1;


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
                    responseBuilder.append("Duplicate txn: Inconsistency in the generated duplicate segments").append("\n");
                }
                isConsistent = isConsistent && duplicateSegments.equals(dup2Segments);

                // Check the reference epochs.
                if (duplicateActiveEpochRecord.getReferenceEpoch() != finalTxnEpochRecord.getReferenceEpoch()) {
                    responseBuilder.append("Duplicate txn: Inconsistency in the reference epochs").append("\n");
                }
                isConsistent = isConsistent && duplicateActiveEpochRecord.getReferenceEpoch() == finalTxnEpochRecord.getReferenceEpoch();
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
                    responseBuilder.append("Duplicate active : Inconsistency in the generated duplicate segments").append("\n");
                }
                isConsistent = isConsistent && duplicateSegments.equals(dup2Segments);

                // Check the reference epochs.
                if (duplicateActiveEpochRecord.getReferenceEpoch() != finalActiveEpochRecord.getReferenceEpoch()) {
                    responseBuilder.append("Duplicate active: Inconsistency in the reference epochs").append("\n");
                }
                isConsistent = isConsistent && duplicateActiveEpochRecord.getReferenceEpoch() == finalActiveEpochRecord.getReferenceEpoch();
            }

        } else {
            responseBuilder.append("Not a rolling transaction so can't say much").append("\n");
            responseBuilder.append("CommittingTransactionRecord : ");
            responseBuilder.append(outputCommittingTransactions(committingRecord));
            isConsistent = true;
        }

        // Based on consistency, return all records or none.
        if (!isConsistent) {
            responseBuilder.append("EpochTransitionRecord : ").append(outputCommittingTransactions(committingRecord));

            responseBuilder.append("Duplicate Active EpochRecord : ").append(outputEpoch(duplicateActiveEpochRecord));
            responseBuilder.append("Duplicate Active HistoryTimeSeriesRecord : ").append(outputHistoryRecord(duplicateActiveHistoryRecord));

            responseBuilder.append("Duplicate Txn EpochRecord : ").append(outputEpoch(duplicateTxnEpochRecord));
            responseBuilder.append("Duplicate Txn HistoryTimeSeriesRecord : ").append(outputHistoryRecord(duplicateTxnHistoryRecord));

            output(responseBuilder.toString());
            return false;
        }

        output("Consistent with respect to committing transaction.\n");
        return true;
    }
}
