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

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.UUID;

/**
 * Represents an update to a value of an Attribute.
 * Mock io.pravega.segmentstore.contracts.AttributeUpdate
 */
@AllArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
@NotThreadSafe
public class AttributeUpdate {
    /**
     * The ID of the Attribute to update.
     */
    private final UUID attributeId;

    /**
     * The UpdateType of the attribute.
     */
    private final AttributeUpdateType updateType;

    /**
     * The new Value of the attribute.
     */
    private long value;

    /**
     * If UpdateType is ReplaceIfEquals, then this is the value that the attribute must currently have before making the
     * update. Otherwise this field is ignored.
     */
    private final long comparisonValue;

    @Override
    public String toString() {
        return String.format("AttributeId = %s, Value = %s, UpdateType = %s", this.attributeId, this.value, this.updateType);
    }
}