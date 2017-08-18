/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegastreamstat.zookeeper;

import io.pravega.common.cluster.Host;
import io.pravega.common.segment.SegmentToContainerMapper;
import io.pravega.tools.pravegastreamstat.logs.bookkeeper.LogMetadata;
import io.pravega.tools.pravegastreamstat.record.ActiveTxnRecord;
import io.pravega.tools.pravegastreamstat.record.CompletedTxnRecord;
import io.pravega.tools.pravegastreamstat.record.SegmentRecord;
import io.pravega.tools.pravegastreamstat.service.ExceptionHandler;
import io.pravega.tools.pravegastreamstat.service.MetadataCollector;
import io.pravega.tools.pravegastreamstat.service.PrintHelper;
import io.pravega.tools.pravegastreamstat.service.StreamStatConfigure;
import io.pravega.tools.pravegastreamstat.service.FormatPrinter;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The implementation of printing zookeeper's metadata.
 * This implementation print out four things in zookeeper.
 * <ul>
 *     <li> Cluster's information in 'cluster' node </li>
 *     <li> Segment's information under 'store' node </li>
 *     <li> Container's log address under 'segmentstore' node </li>
 *     <li> Transactions' metadata under 'transaction' node </li>
 * </ul>
 */
public class ZKPrinterImpl implements FormatPrinter {
    // region configuration

    /**
     * The configuration to use, controls how to print.
     */
    private final StreamStatConfigure conf;

    // endregion

    // region metadata

    /**
     * The singleton instance of metadata collector.
     */
    private MetadataCollector metadataCollector = MetadataCollector.getInstance();

    // endregion

    /**
     * Create a new instance of the zookeeper printer.
     * @param conf The configuration to use.
     */
    public ZKPrinterImpl(StreamStatConfigure conf) {
        this.conf = conf;
    }

    /**
     * Print the data from zookeeper.
     */
    public void print() {
        ZKHelper zk;
        try {
            zk = ZKHelper.create(this.conf.getZkURL(), this.conf.getScope(), this.conf.getStream());
        } catch (ZKConnectionFailedException e) {
            metadataCollector.setHealthZk(false);
            ExceptionHandler.NO_ZK.apply();
            return;
        }

        // region cluster information

        if (conf.isCluster()) {
            PrintHelper.printHead("Information for cluster");
            PrintHelper.println();
        }

        metadataCollector.setClusterName(zk.getClusterName());

        // display all the controllers and their uri
        if (conf.isCluster()) {
            PrintHelper.format("Controllers: %n");
        }

        List<String> controllers = zk.getControllers();
        for (String controller:
                controllers) {
            String[] parts = controller.split(":");
            if (parts.length >= 2) {
                if (conf.isCluster()) {
                    PrintHelper.format("Controller[%s:%s]%n", parts[0], parts[1]);
                }
            }
        }

        // display all the segment stores and containers they hold.
        Map<Host, Set<Integer>> hostMap = zk.getCurrentHostMap();
        if (conf.isCluster()) {
            PrintHelper.format("%nSegment Container mappings:%n");
        }

        int totalContainerCounts = 0;
        for (Host host:
                hostMap.keySet()) {
            if (conf.isCluster()) {
                PrintHelper.format("Segment Store[%s] => Containers %s%n",
                        host.toString(),
                        hostMap.get(host));
            }
            totalContainerCounts += (hostMap.get(host)).size();
        }
        metadataCollector.setContainerCount(totalContainerCounts);

        // endregion

        // region stream segment data

        PrintHelper.format(PrintHelper.Color.BLUE, "%nInformation for Stream %s/%s:%n%n",
                this.conf.getScope(), this.conf.getStream());

        // segments
        PrintHelper.format(PrintHelper.Color.BLUE, "Segments:%n");

        List<SegmentRecord> segmentRecords = zk.getSegmentData();

        for (SegmentRecord segmentRecord:
                segmentRecords) {

            // get the segment records
            Date startTime = new Date(segmentRecord.getStartTime());
            PrintHelper.format("Segment #%s:\tStart Time: %s%n",
                    segmentRecord.getSegmentNumber(),
                    startTime);

            // get the container ID
            SegmentToContainerMapper toContainerMapper = new SegmentToContainerMapper(metadataCollector.getContainerCount());

            String streamSegmentName = String.format("%s/%s/%s", conf.getScope(), conf.getStream(), segmentRecord.getSegmentNumber());
            int containerId = toContainerMapper.getContainerId(streamSegmentName);

            metadataCollector.getSegmentNameToContainerMap().put(streamSegmentName, containerId);

            PrintHelper.format("\t\tContainer Id: %d%n", containerId);

            Optional<Host> host = zk.getHostForContainer(containerId);
            host.ifPresent(
                    x -> PrintHelper.format("\t\tSegment Store host: [%s]", x)
            );
            PrintHelper.format("%n%n");
        }
        if (conf.isTxn()) {
            if (!zk.getTxnExist()) {
                ExceptionHandler.NO_TXN.apply();
            } else {
                Map<String, ActiveTxnRecord> activeTxs = zk.getActiveTxn();
                List<String> completedTxs = zk.getCompleteTxn();

                PrintHelper.println(PrintHelper.Color.PURPLE, "Active transactions: ");
                for (Map.Entry<String, ActiveTxnRecord> activeTx:
                        activeTxs.entrySet()) {
                    PrintHelper.printHead(activeTx.getKey());
                    activeTx.getValue().print();
                }

                PrintHelper.println();
                PrintHelper.println(PrintHelper.Color.PURPLE, "Completed transactions: ");
                for (String completedTx:
                        completedTxs) {
                    byte[] data = zk.getCompleteTxnData(completedTx);
                    CompletedTxnRecord record = CompletedTxnRecord.parse(data);
                    PrintHelper.printHead(completedTx);
                    record.print();

                }
                PrintHelper.println();
            }

        }

        // get the container metadata
        PrintHelper.format(PrintHelper.Color.BLUE, "Container data log metadata: %n");
        for (Integer containerId :
                metadataCollector.getSegmentNameToContainerMap().values()) {
            LogMetadata tmp = zk.getLogMetadata(containerId);
            PrintHelper.format("Container #%d: ", containerId);
            metadataCollector.getContainerToLogMetadataMap().put(containerId, tmp);
            PrintHelper.println(tmp);
        }

        zk.close();
    }
}
