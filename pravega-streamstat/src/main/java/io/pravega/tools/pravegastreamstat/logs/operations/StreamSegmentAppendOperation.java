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
import io.pravega.common.io.StreamHelpers;
import lombok.Getter;
import io.pravega.tools.pravegastreamstat.service.PrintHelper;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;

/**
 * Log Operation that represents a StreamSegment Append. This operation, as opposed from CachedStreamSegmentAppendOperation,
 * can be serialized to a DurableDataLog. This operation (although possible), should not be directly added to the In-Memory Transaction Log.
 */
public class StreamSegmentAppendOperation extends Operation {

    /**
     * The attributes update.
     */
    private Collection<AttributeUpdate> attributeUpdates;

    /**
     * The data to append.
     */
    @Getter
    private byte[] data;
    StreamSegmentAppendOperation(OperationHeader header, DataInputStream source) throws IOException, SerializationException {
        super(header, source);
    }

    // region Operation Implementation

    @Override
    void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        setStreamSegmentId(source.readLong());
        setStreamSegmentOffset(source.readLong());
        int dataLength = source.readInt();
        this.data = new byte[dataLength];
        int bytesRead = StreamHelpers.readAll(source, this.data, 0, this.data.length);
        assert bytesRead == this.data.length : "StreamHelpers.readAll did not read all the bytes requested.";
        this.attributeUpdates = AttributeSerializer.deserializeUpdates(source);
    }

    @Override
    public void print() {
        super.print();
        PrintHelper.print("StreamSegmentOffset", streamSegmentOffset, false);
        PrintHelper.print("AttributeUpdate", attributeUpdates.toString(), true);

    }

    // endregion
}
