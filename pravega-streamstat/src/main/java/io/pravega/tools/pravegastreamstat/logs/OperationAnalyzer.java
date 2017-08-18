/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegastreamstat.logs;

import com.google.common.collect.Iterators;
import io.pravega.tools.pravegastreamstat.service.EventDataAnalyzer;
import io.pravega.tools.pravegastreamstat.service.ExceptionHandler;
import io.pravega.tools.pravegastreamstat.service.MetadataCollector;
import io.pravega.tools.pravegastreamstat.service.PrintHelper;
import io.pravega.tools.pravegastreamstat.service.SerializationException;
import io.pravega.tools.pravegastreamstat.service.StreamStatConfigure;
import io.pravega.tools.pravegastreamstat.logs.bookkeeper.LogMetadata;
import io.pravega.tools.pravegastreamstat.logs.bookkeeper.BookkeeperLogReader;
import io.pravega.tools.pravegastreamstat.logs.operations.MergeTransactionOperation;
import io.pravega.tools.pravegastreamstat.logs.operations.MetadataCheckpointOperation;
import io.pravega.tools.pravegastreamstat.logs.operations.Operation;
import io.pravega.tools.pravegastreamstat.logs.operations.StreamSegmentAppendOperation;
import io.pravega.tools.pravegastreamstat.logs.operations.StreamSegmentMapOperation;
import io.pravega.tools.pravegastreamstat.logs.operations.TransactionMapOperation;
import io.pravega.common.util.ByteArraySegment;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.bookkeeper.client.BookKeeper;
import io.pravega.tools.pravegastreamstat.record.DataRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;


/**
 * Functions for analysing operation log.
 */
@RequiredArgsConstructor
public class OperationAnalyzer {
    private final BookKeeper bkClient;
    private final String segmentName;
    private final LogMetadata log;
    private final Map<String, Long> segmentNameToIdMap;
    private final StreamStatConfigure conf;

    private long totalBytesOfSegment;
    private int totalEventsOfSegment;
    // txn segment id to event number map.
    private Map<Long, Integer> transactionSegments;
    private long offsetStart;
    private long currentOffset;
    private boolean dataContinuity;

    /**
     * Clear all the state logged.
     */
    private void init() {
        totalBytesOfSegment = 0;
        totalEventsOfSegment = 0;
        transactionSegments = new HashMap<>();
        offsetStart = -1;
        currentOffset = 0;
        dataContinuity = true;
        transactionSegments = new HashMap<>();
    }

    /**
     * Print and analyse the log.
     * @throws SerializationException If a sub procedure find a Serialization Error.
     */
    public void printLog() throws SerializationException {

        LogReader reader = new BookkeeperLogReader(bkClient, log, 0);
        if (!conf.isLog() || conf.isExplicit()) {
            PrintHelper.block();
        }
        printLogWithReader(reader);
        PrintHelper.unblock();

        if (!conf.isSimple()) {
            return;
        }

        if (conf.isExplicit()) {
            redo();
        }

        if (!dataContinuity) {
            ExceptionHandler.DATA_NOT_CONTINUOUS.apply();
        }

        val segmentLength = MetadataCollector.getInstance().getSegmentLength();
        if (segmentLength.containsKey(segmentName) &&
                currentOffset < segmentLength.get(segmentName) &&
                currentOffset != 0) {
            ExceptionHandler.T1_OFFSET_LOWER_THAN_T2.apply();
        }

        PrintHelper.print("Total Bytes in T1 log", totalBytesOfSegment, false);
        PrintHelper.print("Total Events in T1 log", totalEventsOfSegment, false);

        if (currentOffset != 0) {
            PrintHelper.print("Offset start", offsetStart < 0 ? 0 : offsetStart, false);
            PrintHelper.print("Offset end", currentOffset, true);
        } else {
            PrintHelper.println("Logs related to this segment are not found.");
        }

    }

    /**
     * Print and analyse the log with given reader.
     * @param reader The log reader to read from.
     * @throws SerializationException When read item fail to deserialize.
     */
    private void printLogWithReader(LogReader reader) throws SerializationException {
        init();
        LogReader.ReadItem item = reader.getNext();

        while (item != null) {
            List<ByteArraySegment> segments = getSegments(item, conf.isSimple());

            Operation operation = getOperation(segments);
            printOperation(operation);
            item = reader.getNext();
        }

        reader.close();

    }

    /**
     * Print the operation according to current segment name
     * @param operation the operation to print
     * @throws SerializationException throw when cannot deserialize data.
     */
    private void printOperation(Operation operation) throws SerializationException {
        if (conf.isSimple()) {
            if (operation instanceof MetadataCheckpointOperation) {
                ContainerMetadataAnalyzer metadataUpdateTransaction =
                        new ContainerMetadataAnalyzer(
                                log.getContainerId(), segmentName);

                if (segmentNameToIdMap.containsKey(segmentName)) {
                    metadataUpdateTransaction.setKnownId(segmentNameToIdMap.get(segmentName));
                }

                try {
                    ContainerMetadataAnalyzer.SegmentMetadata segmentMetadata =
                            metadataUpdateTransaction.deserializeFrom((MetadataCheckpointOperation) operation);
                    if (segmentMetadata != null) {
                        segmentNameToIdMap.put(segmentName, segmentMetadata.getSegmentId());
                        metadataUpdateTransaction.print();
                    }

                } catch (IOException e) {
                    throw new SerializationException("MetadataCheckPointOperation", "Unable to read");
                }

            }
            if (operation instanceof StreamSegmentMapOperation) {
                if (segmentName.equals(((StreamSegmentMapOperation) operation).getStreamSegmentName())) {
                    segmentNameToIdMap.put(segmentName, operation.getStreamSegmentId());
                    operation.print();
                }
            } else if (operation instanceof TransactionMapOperation) {
                Long segmentId = segmentNameToIdMap.get(segmentName);

                if (segmentId != null &&
                        segmentId.equals(
                                ((TransactionMapOperation) operation)
                                        .getParentStreamSegmentId())) {
                    transactionSegments.put(operation.getStreamSegmentId(), 0);
                    operation.print();
                }
            } else {
                Long segmentId = segmentNameToIdMap.get(segmentName);

                if (segmentId != null &&
                        (operation.getStreamSegmentId() == segmentId ||
                                transactionSegments.keySet().contains(operation.getStreamSegmentId()))) {
                    operation.print();
                    processOperation(operation);

                }
            }
        } else {
            operation.print();
            processOperation(operation);
        }
    }

    /**
     * Print Data of the append operation and add statistics about the operation
     * @param operation the operation to print
     * @throws SerializationException throw when cannot serialize data
     */
    private void processOperation(Operation operation) throws SerializationException {
        if (operation instanceof StreamSegmentAppendOperation) {
            byte[] data = ((StreamSegmentAppendOperation) operation).getData();

            DataRecord dataRecord = EventDataAnalyzer.getDataRecord(data, conf.isData());

            if (dataRecord.getLength() != data.length) {
                throw new SerializationException("Append Operation Data", "Data not consistent, some byte maybe lost.");
            }

            PrintHelper.println(PrintHelper.Color.PURPLE, dataRecord.getToPrint());
            PrintHelper.println();

            if (conf.isSimple()) {
                updateStat(operation.getStreamSegmentOffset(), operation.getStreamSegmentId(),
                        dataRecord.getLength(), dataRecord.getEvents());
            }

        }

        if (conf.isSimple() && operation instanceof MergeTransactionOperation) {
            long transactionSegmentId = ((MergeTransactionOperation) operation).getTransactionSegmentId();
            int events = transactionSegments.get(transactionSegmentId);

            if (conf.isSimple()) {
                updateStat(operation.getStreamSegmentOffset(), operation.getStreamSegmentId(),
                        ((MergeTransactionOperation) operation).getLength(), events);
            }
        }
    }

    /**
     * Update the statistic value (bytes and event).
     * @param offset current operation offset.
     * @param operationSegmentId operations segment id (used to check origin segment or txn segment).
     * @param length length of the operation data.
     * @param events number of events in operation.
     */
    private void updateStat(long offset, long operationSegmentId, long length, int events) {

        Long segmentId = segmentNameToIdMap.get(segmentName);

        if (segmentId != null &&
                operationSegmentId != segmentId) {
            int currentTxEvents = transactionSegments.get(operationSegmentId);

            transactionSegments.put(operationSegmentId, events + currentTxEvents);

        } else {

            totalBytesOfSegment += length;
            totalEventsOfSegment += events;

            if (offsetStart < 0) {
                offsetStart = offset;
                currentOffset = offsetStart;
            }

            if (currentOffset != offset) {
                dataContinuity = false;
            }

            currentOffset += length;
        }
    }

    /**
     * reprint only if there is data missing
     * @throws SerializationException If unable to serialize some content.
     */
    private void redo() throws SerializationException {
        LogReader reader;

        val segmentLength = MetadataCollector.getInstance().getSegmentLength();
        if (!dataContinuity ||
                currentOffset <
                        segmentLength.get(segmentName)) {
            PrintHelper.println();
            PrintHelper.println(PrintHelper.Color.RED, "Some log may have not been read, use long polling read to read explicit io.pravega.tools.pravegastreamstat.logs");
            PrintHelper.println();

            reader = new BookkeeperLogReader(bkClient, log, 1);
        } else {
            reader = new BookkeeperLogReader(bkClient, log, 0);
        }

        if (!conf.isLog()) {
            PrintHelper.block();
        }
        printLogWithReader(reader);
        PrintHelper.unblock();

    }

    /**
     * Get operation from byte array list.
     * @param segments the byte array segments hold one operation.
     * @return operation
     * @throws SerializationException return when cannot deserialize operation.
     */
    private static Operation getOperation(List<ByteArraySegment> segments) throws SerializationException {
        Stream<InputStream> ss = segments
                .stream()
                .map(ByteArraySegment::getReader);
        return Operation.deserialize(
                new SequenceInputStream(
                        Iterators.asEnumeration(
                                ss.iterator()
                        )
                )
        );
    }

    /**
     * Get operations from data frame. Operation can be in multiple entries.
     * @param item the item that hold the data frame
     * @param simple determine whether to print info of data entries.
     * @return the list of byte array hold one operation
     * @throws SerializationException return when unable to deserialize entry header
     */
    private static List<ByteArraySegment> getSegments(LogReader.ReadItem item, boolean simple) throws SerializationException {

        DataFrame frame = new DataFrame(item);
        List<ByteArraySegment> segments = new LinkedList<>();

        while (true) {
            DataFrame.DataEntry segment = frame.getEntry();
            if (segment == null) {
                throw new SerializationException("Entry", "Unable to serialize data frame entry");
            }
            if (!simple) {
                segment.print();
            }

            segments.add(segment.getData());

            if (segment.isLastRecordEntry()) {
                break;
            }
        }
        return segments;
    }


}
