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

    public String toString() {
        if (this.recordType == EpochRecord.class) {
            return outputEpoch((EpochRecord) this.record);
        }

        if (this.recordType == HistoryTimeSeriesRecord.class) {
            return outputHistoryRecord((HistoryTimeSeriesRecord) this.record);
        }

        if (this.recordType == StreamConfigurationRecord.class) {
            return outputConfiguration((StreamConfigurationRecord)this.record);
        }

        if (this.recordType == EpochTransitionRecord.class) {
            return outputTransition((EpochTransitionRecord)this.record);
        }

        if (this.recordType == CommittingTransactionsRecord.class) {
            return outputCommittingTransactions((CommittingTransactionsRecord) this.record);
        }

        if (this.recordType == StreamTruncationRecord.class) {
            return outputTruncation((StreamTruncationRecord)this.record);
        }

        return null;
    }
}
