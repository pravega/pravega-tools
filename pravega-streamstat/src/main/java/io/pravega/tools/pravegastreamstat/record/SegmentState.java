/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegastreamstat.record;

import lombok.Data;
import io.pravega.tools.pravegastreamstat.service.PrintHelper;

import java.util.Map;
import java.util.UUID;

/**
 * Segment states (stored in HDFS).
 */
@Data
public class SegmentState {
    private final byte version;
    private final long segmentId;
    private final String segmentName;
    private final Map<UUID, Long> attributes;

    /**
     * Print out the segment state.
     */
    public void print() {
        PrintHelper.printHead("Segment State");
        PrintHelper.print("SegmentId", segmentId == Long.MIN_VALUE ? "MISSING" : String.format("%d", segmentId), false);
        PrintHelper.print("SegmentName", segmentName, false);
        PrintHelper.print(attributes);
    }
}
