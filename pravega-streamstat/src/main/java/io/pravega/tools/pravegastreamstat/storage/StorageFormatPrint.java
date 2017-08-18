/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegastreamstat.storage;

import lombok.val;
import io.pravega.tools.pravegastreamstat.record.SegmentRecord;
import io.pravega.tools.pravegastreamstat.service.ExceptionHandler;
import io.pravega.tools.pravegastreamstat.service.FormatPrinter;
import io.pravega.tools.pravegastreamstat.service.MetadataCollector;
import io.pravega.tools.pravegastreamstat.service.PrintHelper;
import io.pravega.tools.pravegastreamstat.service.StreamStatConfigure;
import io.pravega.tools.pravegastreamstat.zookeeper.ZKConnectionFailedException;
import io.pravega.tools.pravegastreamstat.zookeeper.ZKHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.stream.IntStream;

public abstract class StorageFormatPrint implements FormatPrinter {

    protected StreamStatConfigure conf;
    @Override
    abstract public void print();

    /**
     * Ref: io.pravega.controller.store.stream.PersistStreamBase.createNewSegmentTable.
     * Recover the segment node in zookeeper
     */
    protected void recoverSegment() {
        PrintHelper.processStart("Recover zookeeper's segment metadata node");
        val segments = MetadataCollector.getInstance().getSegments();
        final int numSegments = segments.size();
        final double keyRangeChunk = 1.0 / numSegments;

        final int startingSegmentNumber = 0;
        final long timeStamp = new Date().getTime();
        ByteArrayOutputStream segmentStream = new ByteArrayOutputStream();
        IntStream.range(startingSegmentNumber, numSegments)
                .forEach(value -> {
                    try {
                        segmentStream.write(
                                new SegmentRecord(startingSegmentNumber + value,
                                        timeStamp, value * keyRangeChunk,
                                        (value +  1) * keyRangeChunk
                                ).toByteArray());
                    } catch (IOException e) {
                        throw new RuntimeException();
                    }
                });

        ZKHelper zk;
        try {
            zk = ZKHelper.create(this.conf.getZkURL(), this.conf.getScope(), this.conf.getStream());
            zk.updateSegmentTable(segmentStream.toByteArray());

            PrintHelper.processEnd();
        } catch (ZKConnectionFailedException e) {
            PrintHelper.processEnd();
            ExceptionHandler.NO_ZK.apply();
        }
    }
}
