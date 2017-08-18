/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegastreamstat.logs.operations;

import io.pravega.tools.pravegastreamstat.service.SerializationException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Defines Types of Log Operations.
 */
@Getter(AccessLevel.PACKAGE)
@RequiredArgsConstructor
public enum OperationType {
    Probe((byte) 0, null), // This operation cannot be serialized.
    Append((byte) 1, StreamSegmentAppendOperation::new),
    Seal((byte) 2, StreamSegmentSealOperation::new),
    Merge((byte) 3, MergeTransactionOperation::new),
    SegmentMap((byte) 4, StreamSegmentMapOperation::new),
    TransactionMap((byte) 5, TransactionMapOperation::new),
    MetadataCheckpoint((byte) 6, MetadataCheckpointOperation::new),
    UpdateAttributes((byte) 7, UpdateAttributesOperation::new),
    StorageMetadataCheckpoint((byte) 8, StorageMetadataCheckpointOperation::new);

    final byte type;
    final DeserializationConstructor deserializationConstructor;

    @FunctionalInterface
    interface DeserializationConstructor {
        Operation apply(Operation.OperationHeader header, DataInputStream source) throws IOException, SerializationException;
    }

    public static OperationType getOperationType(byte type) {
        for (OperationType operationType:
            OperationType.class.getEnumConstants()) {
            if (operationType.type == type) {
                return operationType;
            }
        }

        return Probe;
    }

}
