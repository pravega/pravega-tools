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
import io.pravega.common.util.ByteArraySegment;
import io.pravega.tools.pravegastreamstat.service.PrintHelper;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Log Operation that contains a checkpoint of the Metadata at a particular point in time that contains only information
 * about the Storage state of Segments (essentially those items that are not updated using regular Log Operations).
 */
public class StorageMetadataCheckpointOperation extends Operation {

    /**
     * The content of the metadata.
     */
    private ByteArraySegment contents;

    StorageMetadataCheckpointOperation(Operation.OperationHeader header, DataInputStream source) throws IOException, SerializationException {
        super(header, source);
    }

    // region Operation implementation

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        int contentsLength = source.readInt();
        this.contents = new ByteArraySegment(new byte[contentsLength]);
        int bytesRead = this.contents.readFrom(source);
        assert bytesRead == contentsLength : "StreamHelpers.readAll did not read all the bytes requested.";
    }

    @Override
    public void print() {
        super.print();
        PrintHelper.print("MetadataLength", contents.getLength(), true);
    }

    // endregion
}
