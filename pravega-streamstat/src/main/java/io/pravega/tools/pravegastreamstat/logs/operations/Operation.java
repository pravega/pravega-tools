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
import lombok.Data;
import io.pravega.tools.pravegastreamstat.service.PrintHelper;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Base class for a Log Operation.
 */
@Data
public abstract class Operation {
    static final byte CURRENT_VERSION = 0;

    OperationHeader header;
    byte version;
    long streamSegmentId;
    long streamSegmentOffset;

    /**
     * Create a new instance of the Operation class using the given header and source.
     *
     * @param header The operation header to use.
     * @param source A data stream to deserialize from.
     * @throws IOException              If the data stream throws one.
     * @throws SerializationException   If the content cannot deserialize.
     */
    Operation(OperationHeader header, DataInputStream source) throws IOException, SerializationException {
        this.header = header;
        deserializeContent(source);
    }

    /**
     * A static method to deserialize an input stream to operation.
     * @param input the data stream to read from, with operation and its header.
     * @return the deserialize operation.
     * @throws SerializationException If the content cannot deserialize to operation.
     */
    public static Operation deserialize(InputStream input) throws SerializationException {
        DataInputStream source = new DataInputStream(input);
        try {
            OperationHeader header = new OperationHeader(source);
            OperationType type = OperationType.getOperationType(header.getType());
            if (type == OperationType.Probe) {
                throw new SerializationException("Operation.deserialize", "Invalid operation type");
            }
            return type.deserializationConstructor.apply(header, source);
        } catch (IOException e) {
            throw new SerializationException("Operation.deserialize", "Unable to deserialize");
        }

    }

    /**
     * Deserialize the content of this operation.
     *
     * @param source the data stream to read from.
     * @throws IOException              Throw from the DataInputStream.
     * @throws SerializationException   If the content cannot deserialize.
     */
    abstract void deserializeContent(DataInputStream source) throws IOException, SerializationException;


    /**
     * Reads a version byte from the given input stream and compares it to the given expected version.
     *
     * @param source          The input stream to read from.
     * @param expectedVersion The expected version to compare to.
     * @throws IOException            If the input stream threw one.
     * @throws SerializationException If the versions mismatched.
     */
    void readVersion(DataInputStream source, byte expectedVersion) throws IOException, SerializationException {
        version = source.readByte();
        if (version != expectedVersion) {
            throw new SerializationException(String.format("%s.deserialize", this.getClass().getSimpleName()), String.format("Unsupported version: %d.", version));
        }
    }


    /**
     * Header for a serialized operation.
     */
    @Data
    public static class OperationHeader {

        /**
         * The header's version.
         */
        final byte version;

        /**
         * The type byte of operation.
         */
        final byte type;

        /**
         * The sequence number for the operation.
         */
        final long sequenceNumber;

        /**
         * Create a new instance of the Operation class.
         * @param source The DataInputStream to deserialize from.
         * @throws IOException If the data stream throws one.
         */
        OperationHeader(DataInputStream source) throws IOException {
            version = source.readByte();
            type = source.readByte();
            sequenceNumber = source.readLong();
        }
    }

    /**
     * Print the operation's information.
     */
    public void print() {

        PrintHelper.printHead("Operation");
        PrintHelper.print("Type", OperationType.getOperationType(header.getType()).toString(), false);
        PrintHelper.print("Sequence", header.getSequenceNumber(), false);
        PrintHelper.print("StreamSegmentId", streamSegmentId, false);

    }
}


