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

import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;

public class EpochHistoryCrossCheck {

    public static boolean checkConsistency(EpochRecord record, HistoryTimeSeriesRecord history) {
        StringBuilder responseBuilder = new StringBuilder();
        boolean isConsistent;

        if (record == null || history == null) {
            return false;
        }

        isConsistent = record.getEpoch() == history.getEpoch();
        if (record.getEpoch() != history.getEpoch()) {
            responseBuilder.append("Epoch mismatch : May or may not be the correct record").append("\n");
        }

        isConsistent = isConsistent & record.getReferenceEpoch() == history.getReferenceEpoch();
        if (record.getReferenceEpoch() != history.getReferenceEpoch()) {
            responseBuilder.append("Reference epoch mismatch.").append("\n");
        }

        isConsistent = isConsistent & record.getSegments().equals(history.getSegmentsCreated());
        if (!record.getSegments().equals(history.getSegmentsCreated())) {
            responseBuilder.append("Segment data mismatch.").append("\n");
        }

        isConsistent = isConsistent & record.getCreationTime() == history.getScaleTime();
        if (record.getCreationTime() != history.getScaleTime()) {
            responseBuilder.append("Creation time mismatch.").append("\n");
        }

        if (!isConsistent) {
            System.out.println(responseBuilder.toString());
        }
        return isConsistent;
    }
}
