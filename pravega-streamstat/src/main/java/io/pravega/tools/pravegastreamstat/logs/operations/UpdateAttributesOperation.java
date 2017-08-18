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
import java.util.Collection;

/**
 * Log Operation that represents an Update to a Segment's Attribute collection.
 */
public class UpdateAttributesOperation extends Operation {

    /**
     * The attribute to update.
     */
    private Collection<AttributeUpdate> attributeUpdates;

    UpdateAttributesOperation(OperationHeader header, DataInputStream source) throws IOException, SerializationException {
        super(header, source);
    }

    // region Operation Implementation

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        this.streamSegmentId = source.readLong();
        this.attributeUpdates = AttributeSerializer.deserializeUpdates(source);
    }

    @Override
    public void print() {
        super.print();

        PrintHelper.print("AttributeUpdates", attributeUpdates.toString(), true);
    }

    // region Operation Implementation

}
