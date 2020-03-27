/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli.commands.troubleshoot;

import io.pravega.controller.store.stream.records.*;
import lombok.Builder;
import lombok.Getter;

import java.lang.reflect.Type;

import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.*;

/**
 * A wrapper class to store various types of metadata records.
 */
public class Record<T> {

    @Getter
    private final T record;
    @Getter
    private final Type recordType;

    @Builder
    public Record(T record, Type recordType) {
        this.record = record;
        this.recordType = recordType;
    }

    /**
     * Method to write the record as a string.
     *
     * @return a String containing record information
     */
    public String toString() {
        StringBuilder responseBuilder =  new StringBuilder();
        responseBuilder.append(this.recordType).append("\n");

        if (this.recordType == EpochRecord.class) {
            return responseBuilder.append(outputEpoch((EpochRecord) this.record)).toString();
        }

        if (this.recordType == HistoryTimeSeriesRecord.class) {
            return responseBuilder.append(outputHistoryRecord((HistoryTimeSeriesRecord) this.record)).toString();
        }

        if (this.recordType == StreamConfigurationRecord.class) {
            return responseBuilder.append(outputConfiguration((StreamConfigurationRecord) this.record)).toString();
        }

        if (this.recordType == EpochTransitionRecord.class) {
            return responseBuilder.append(outputTransition((EpochTransitionRecord) this.record)).toString();
        }

        if (this.recordType == CommittingTransactionsRecord.class) {
            return responseBuilder.append(outputCommittingTransactions((CommittingTransactionsRecord) this.record)).toString();
        }

        if (this.recordType == StreamTruncationRecord.class) {
            return responseBuilder.append(outputTruncation((StreamTruncationRecord) this.record)).toString();
        }

        return responseBuilder.toString();
    }
}
