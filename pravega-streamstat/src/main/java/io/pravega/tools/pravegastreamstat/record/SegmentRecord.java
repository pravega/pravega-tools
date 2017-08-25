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

import io.pravega.common.util.BitConverter;
import lombok.Data;

/**
 * get segment record from ZK.
 */
@Data
public class SegmentRecord {
    public static final int SEGMENT_RECORD_SIZE = Integer.BYTES + Long.BYTES + Double.BYTES + Double.BYTES;

    private final int segmentNumber;
    private final long startTime;
    private final double routingKeyStart;
    private final double routingKeyEnd;

    /**
     * Deserialize Segment Record from table.
     * @param table A table of segment records.
     * @param offset Starting offset of the SegmentRecord in the table.
     * @return The result SegmentRecord.
     */
    public static SegmentRecord parse(final byte[] table, int offset) {
        return new SegmentRecord(BitConverter.readInt(table, offset),
               BitConverter.readLong(table, Integer.BYTES + offset),
                toDouble(table, Integer.BYTES + Long.BYTES + offset),
                toDouble(table, Integer.BYTES + Long.BYTES + Double.BYTES + offset));
    }

    /**
     * Convert the segment record to byte array.
     * @return Result byte array.
     */
    public byte[] toByteArray() {
        byte[] b = new byte[SEGMENT_RECORD_SIZE];
        BitConverter.writeInt(b, 0, segmentNumber);
        BitConverter.writeLong(b, Integer.BYTES, startTime);
        BitConverter.writeLong(b, Integer.BYTES + Long.BYTES, Double.doubleToRawLongBits(routingKeyStart));
        BitConverter.writeLong(b, Integer.BYTES + Long.BYTES * 2, Double.doubleToRawLongBits(routingKeyEnd));
        return b;
    }

    /**
     * An utility to convert table byte to a double number.
     * @param b        Byte array table.
     * @param offset   Offset of the double.
     * @return Result double number.
     */
    private static double toDouble(byte[] b, int offset) {
        return Double.longBitsToDouble(BitConverter.readLong(b, offset));
    }




}
