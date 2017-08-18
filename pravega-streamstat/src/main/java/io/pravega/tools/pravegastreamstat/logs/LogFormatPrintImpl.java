/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegastreamstat.logs;

import io.pravega.tools.pravegastreamstat.logs.bookkeeper.LogMetadata;
import lombok.val;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import io.pravega.tools.pravegastreamstat.service.ExceptionHandler;
import io.pravega.tools.pravegastreamstat.service.FormatPrinter;
import io.pravega.tools.pravegastreamstat.service.MetadataCollector;
import io.pravega.tools.pravegastreamstat.service.PrintHelper;
import io.pravega.tools.pravegastreamstat.service.SerializationException;
import io.pravega.tools.pravegastreamstat.service.StreamStatConfigure;
import io.pravega.tools.pravegastreamstat.zookeeper.ZKHelper;

/**
 * The implementation of the printing tier-1 log's info and collect metadata using the metadata collector.
 */
public class LogFormatPrintImpl implements FormatPrinter {
    // region configuration

    /**
     * The configuration of the service.
     */
    private final StreamStatConfigure conf;

    // endregion

    // region metadata

    /**
     * The singleton instance of the metadata collector.
     */
    private MetadataCollector metadataCollector = MetadataCollector.getInstance();

    // endregion

    /**
     * Constructor to get the log printer with given configuration.
     * @param conf The configuration.
     */
    public LogFormatPrintImpl(StreamStatConfigure conf) {
        this.conf = conf;
    }

    @Override
    public void print() {

        // if configure shows not to print T1 log, skip
        if (!metadataCollector.isHealthZk()) {
            return;
        }

        PrintHelper.println(PrintHelper.Color.PURPLE, "REGION TIER-1 LOGS:");
        ClientConfiguration conf = new ClientConfiguration();

        conf.setZkServers(this.conf.getZkURL());
        conf.setZkLedgersRootPath(String.format(ZKHelper.BK_PATH, metadataCollector.getClusterName()));

        // fetch the maps from metadata to use
        val containerToLogMetadataMap = metadataCollector.getContainerToLogMetadataMap();
        val segmentNameToContainerMap = metadataCollector.getSegmentNameToContainerMap();
        val segmentNameToIdMap = metadataCollector.getSegmentNameToIdMap();

        // start the bk client
        try (BookKeeper bkClient = new BookKeeper(conf)) {
            for (String segmentName:
                    segmentNameToContainerMap.keySet()) {

                LogMetadata logMetadata = containerToLogMetadataMap.get(segmentNameToContainerMap.get(segmentName));
                OperationAnalyzer operationAnalyzer = new OperationAnalyzer(bkClient,
                        segmentName, logMetadata,
                        segmentNameToIdMap, this.conf);
                PrintHelper.println();
                PrintHelper.printHead("Data for segment");
                PrintHelper.print(String.format("%s%n%n", segmentName));

                operationAnalyzer.printLog();

                // if we cannot found segment id in T1 and Storage
                if (this.conf.isSimple() &&
                        !segmentNameToIdMap.keySet()
                                .contains(segmentName)) {
                    ExceptionHandler.NO_SEGMENT_ID.apply();
                }

            }
        } catch (SerializationException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(-1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
