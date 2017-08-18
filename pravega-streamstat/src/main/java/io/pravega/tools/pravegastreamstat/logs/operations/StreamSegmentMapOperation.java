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
import lombok.Getter;
import io.pravega.tools.pravegastreamstat.service.PrintHelper;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Log Operation that represents a mapping of StreamSegment Name to a StreamSegment Id.
 */
@Getter
public class StreamSegmentMapOperation extends Operation {

    /**
     * The name of segment.
     */
    private String streamSegmentName;

    /**
     * Current length of the segment.
     */
    private long length;

    /**
     * If the segment is sealed.
     */
    private boolean sealed;

    /**
     * The segments current attributes.
     */
    private Map<UUID, Long> attributes;

    StreamSegmentMapOperation(Operation.OperationHeader header, DataInputStream source) throws IOException, SerializationException {
        super(header, source);
    }

    // region Operation Implementation

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        this.streamSegmentId = source.readLong();
        this.streamSegmentName = source.readUTF();
        this.length = source.readLong();
        this.sealed = source.readBoolean();
        this.attributes = AttributeSerializer.deserialize(source);
    }

    @Override
    public void print() {
        super.print();
        PrintHelper.print("StreamSegmentName", streamSegmentName, false);
        PrintHelper.print("Length", length, false);
        PrintHelper.print("Sealed", sealed, false);
        PrintHelper.print(attributes);
    }

    // endregion

}
