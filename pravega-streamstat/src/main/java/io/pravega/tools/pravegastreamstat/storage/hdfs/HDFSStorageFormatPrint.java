/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegastreamstat.storage.hdfs;

import lombok.val;
import io.pravega.tools.pravegastreamstat.service.ExceptionHandler;
import io.pravega.tools.pravegastreamstat.service.PrintHelper;
import io.pravega.tools.pravegastreamstat.service.SerializationException;
import io.pravega.tools.pravegastreamstat.service.MetadataCollector;
import io.pravega.tools.pravegastreamstat.service.StreamStatConfigure;
import io.pravega.tools.pravegastreamstat.storage.StorageFormatPrint;

import java.io.IOException;

/**
 * StorageFormatPrint adaptor for a backing HDFS store.
 * Print out all the data for the stream stored in HDFS.
 */
public class HDFSStorageFormatPrint extends StorageFormatPrint {

    /**
     * The singleton instance for the metadata collector.
     */
    private final MetadataCollector metadataCollector = MetadataCollector.getInstance();

    /**
     * Create a new instance for HDFS storage format print.
     * @param configure The configuration instance to use.
     */
    public HDFSStorageFormatPrint(StreamStatConfigure configure) {
        this.conf = configure;
    }

    @Override
    public void print() {
        PrintHelper.println();
        PrintHelper.println(PrintHelper.Color.PURPLE, "REGION TIER-2 HDFS DATA: ");
        PrintHelper.println();

        // fetch some metadata from collector.
        val segmentNames = metadataCollector.getSegmentNameToContainerMap().keySet();
        val segmentNameToIdMap = metadataCollector.getSegmentNameToIdMap();

        boolean needRecover = false;
        try (HDFSHelper hdfsHelper = new HDFSHelper(this.conf)) {
            String streamName = String.format("%s/%s", conf.getScope(), conf.getStream());
            val segments = hdfsHelper.getSegments(streamName);

            // update the MetadataCollector
            metadataCollector.getSegments().addAll(segments);

            for (String segmentName:
                    segments) {
                if (!segmentNames.contains(segmentName)) {
                    if (!conf.isRecover()) {
                        ExceptionHandler.METADATA_NOT_CONSISTENT.apply();
                    } else {
                        needRecover = true;
                    }
                }
                PrintHelper.println();
                PrintHelper.printHead("Data for segment");
                PrintHelper.print(String.format("%s%n%n", segmentName));

                try {
                    hdfsHelper.printSegmentState(segmentName, segmentNameToIdMap);
                    hdfsHelper.printAll(segmentName, conf.isData());
                } catch (HDFSException e) {
                    PrintHelper.processEnd();
                    PrintHelper.printError("Error: " + e.getMessage());
                }

            }

            if (needRecover) {
                recoverSegment();
            }

        } catch (SerializationException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(-1);
        } catch (IOException e) {
            ExceptionHandler.HDFS_CANNOT_CONNECT.apply();
        }
    }
}
