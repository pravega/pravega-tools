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
import io.pravega.tools.pravegastreamstat.service.PrintHelper;

import java.util.Date;

/**
 * The record for the active transactions.
 */
@Data
public class ActiveTxnRecord {
    private static final int ACTIVE_TXN_RECORD_SIZE = 4 * Long.BYTES + Integer.BYTES;
    private final long txCreationTimestamp;
    private final long leaseExpiryTime;
    private final long maxExecutionExpiryTime;
    private final long scaleGracePeriod;
    private final TxnStatus txnStatus;
    private final int segment;

    /**
     * Deserialize the active transaction record from byte array.
     * @param bytes the byte array to deserialize.
     * @param segment The parent segment number of the transaction
     * @return The active transaction record.
     */
    public static ActiveTxnRecord parse(final byte[] bytes, int segment) {
        final int longSize = Long.BYTES;

        final long txCreationTimestamp = BitConverter.readLong(bytes, 0);

        final long leaseExpiryTime = BitConverter.readLong(bytes, longSize);

        final long maxExecutionExpiryTime = BitConverter.readLong(bytes, 2 * longSize);

        final long scaleGracePeriod = BitConverter.readLong(bytes, 3 * longSize);

        final TxnStatus status = TxnStatus.values()[BitConverter.readInt(bytes, 4 * longSize)];

        return new ActiveTxnRecord(txCreationTimestamp, leaseExpiryTime, maxExecutionExpiryTime, scaleGracePeriod, status, segment);
    }

    /**
     * Print the transaction's metadata.
     */
    public void print() {
        PrintHelper.print("txCreationTimestamp", new Date(txCreationTimestamp), false);
        PrintHelper.print("leaseExpiryTime", new Date(leaseExpiryTime), false);
        PrintHelper.print("maxExecutionExpiryTime", new Date(maxExecutionExpiryTime), false);
        PrintHelper.print("scaleGracePeriod", scaleGracePeriod, false);
        PrintHelper.print("txnStatus", txnStatus, false);
        PrintHelper.print("epoch", segment, true);
    }
}