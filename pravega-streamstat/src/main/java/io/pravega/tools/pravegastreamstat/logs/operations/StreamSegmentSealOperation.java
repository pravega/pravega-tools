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
import io.pravega.tools.pravegastreamstat.service.PrintHelper;

import java.io.DataInputStream;
import java.io.IOException;

/**
 *  Log Operation that indicates a StreamSegment has been sealed.
 */
public class StreamSegmentSealOperation extends Operation {

    StreamSegmentSealOperation(Operation.OperationHeader header, DataInputStream source) throws IOException, SerializationException {
        super(header, source);
    }

    // region Operation Implementation

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        setStreamSegmentId(source.readLong());
        this.streamSegmentOffset = source.readLong();
    }

    @Override
    public void print() {
        super.print();
        PrintHelper.print("StreamSegmentOffset", streamSegmentOffset, true);
    }

    // endregion
}
