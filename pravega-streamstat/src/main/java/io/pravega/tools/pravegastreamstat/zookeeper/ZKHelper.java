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

import io.pravega.tools.pravegastreamstat.logs.bookkeeper.LogMetadata;
import io.pravega.common.cluster.Host;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import io.pravega.tools.pravegastreamstat.record.ActiveTxnRecord;
import io.pravega.tools.pravegastreamstat.record.SegmentRecord;
import io.pravega.tools.pravegastreamstat.service.ExceptionHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * Zookeeper helper functions.
 */
public class ZKHelper {

    // region constants

    public static final String BK_PATH = "/pravega/%s/bookkeeper/ledgers";

    private static final String CONTROLLER_PATH = "/cluster/controllers";
    private static final String HOST_MAP_PATH = "/cluster/segmentContainerHostMapping";
    private static final String STREAM_PATH = "/store/%s/%s";
    private static final String SEGMENT_PATH = STREAM_PATH + "/segment";
    private static final String LOG_METADATA_BASE = "/segmentstore/containers";
    private static final int DEFAULT_DEPTH = 2;
    private static final int DIVISOR = 10;
    private static final String SEPARATOR = "/";
    private static final String TXN_PATH = "/transactions";
    private static final String ACTIVE_TX_ROOT = TXN_PATH + "/activeTx";
    private static final String ACTIVE_TX_PATH = ACTIVE_TX_ROOT + "/%s/%s";
    private static final String COMPLETE_TX_ROOT = TXN_PATH + "/completedTx";
    private static final String COMPLETE_TX_PATH = COMPLETE_TX_ROOT + "/%s/%s";

    // endregion

    // region instance variables

    private CuratorFramework zkClient;
    private String segmentPath;
    private String streamPath;
    private String clusterName;
    private String activeTxPath;
    private String completeTxPath;

    // endregion

    // region constructor

    /**
     * Create a new instance of the ZKHelper class.
     * @param zkURL The address of this helper instance connect to.
     * @param scope The scope the target stream is in.
     * @param stream The target stream to get status data.
     * @throws ZKConnectionFailedException If cannot connect to the given address.
     */
    private ZKHelper(String zkURL, String scope, String stream) throws ZKConnectionFailedException {
        segmentPath = String.format(SEGMENT_PATH, scope, stream);
        streamPath = String.format(STREAM_PATH, scope, stream);
        activeTxPath = String.format(ACTIVE_TX_PATH, scope, stream);
        completeTxPath = String.format(COMPLETE_TX_PATH, scope, stream);
        createZKClient(zkURL);
        checkStreamExist();
    }

    // endregion

    /**
     * Update the zookeeper's segment metadata in recover mode.
     * @param data The new segment table to store.
     */
    public void updateSegmentTable(byte[] data) {
        try {
            zkClient.setData().forPath(segmentPath, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the cluster name.
     * @return The name of the cluster.
     */
    String getClusterName() {
        return clusterName;
    }

    /**
     * Get the list of controllers under 'cluster' node.
     * @return A list of controllers.
     */
    List<String> getControllers() {
        return getChild(CONTROLLER_PATH);
    }

    /**
     * Get a list of SegmentRecord with the object's stream.
     * @return The list of SegmentRecord related to the target stream.
     */
    List<SegmentRecord> getSegmentData() {
        byte[] data = getData(segmentPath);

        List<SegmentRecord> ret = new ArrayList<>();
        for (int i = 0; i < data.length; i += SegmentRecord.SEGMENT_RECORD_SIZE) {
            ret.add(SegmentRecord.parse(data, i));
        }
        return ret;
    }

    /**
     * Get the host with given container.
     * @param containerId The target container's id.
     * @return Host of the container.
     */
    Optional<Host> getHostForContainer(int containerId) {

        Map<Host, Set<Integer>> mapping = getCurrentHostMap();

        return mapping
                .entrySet()
                .stream()
                .filter(x -> x.getValue()
                        .contains(containerId))
                .map(Map.Entry::getKey)
                .findAny();

    }

    /**
     * Get if there is a transaction node, check this before get transaction metadata to avoid NodeNotExistException.
     * @return A flag about whether the transaction node exists.
     */
    boolean getTxnExist() {
        return getChild("/").contains(TXN_PATH.replace("/", ""));
    }

    /**
     * Get the list of segments that have active transactions.
     * @return A list of segments that have active transactions. It's safe to check their sub node.
     */
    private List<String> getActiveTxnSegments() {
        return getChild(activeTxPath);
    }

    /**
     * Get the list of active transactions.
     * @return A collection of active transactions. Map from transaction UUID to ActiveTxnRecord.
     */
    Map<String, ActiveTxnRecord> getActiveTxn() {
        Map<String, ActiveTxnRecord> ret = new HashMap<>();

        List<String> segments = getActiveTxnSegments();
        for (String segment:
             segments) {
            List<String> txns = getChild(String.format("%s/%s", activeTxPath, segment));

            for (String txn:
                 txns) {
                byte[] data = getData(String.format("%s/%s/%s", activeTxPath, segment, txn));
                ActiveTxnRecord record = ActiveTxnRecord.parse(data, Integer.parseInt(segment));

                ret.put(txn, record);
            }
        }
        return ret;

    }

    /**
     * Get the list of completed transactions.
     * @return A list of completed transactions.
     */
    List<String> getCompleteTxn() {
        try {
            return zkClient.getChildren().forPath(completeTxPath);
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    /**
     * Get the byte array of metadata of a completed transaction.
     * @param completeTxUUID The UUID of the completed transaction.
     * @return The byte array of its metadata.
     */
    byte[] getCompleteTxnData(String completeTxUUID) {
        return getData(String.format("%s/%s", completeTxPath, completeTxUUID));
    }

    /**
     * Get the host to container map.
     * @return A map from segment store host to containers it holds.
     */
    Map<Host, Set<Integer>> getCurrentHostMap() {
        Map tmp = (Map) SerializationUtils.deserialize(getData(HOST_MAP_PATH));
        Map<Host, Set<Integer>> ret = new HashMap<>();
        for (Object key:
                tmp.keySet()) {
            if (key instanceof Host && tmp.get(key) instanceof Set) {
                Set<Integer> tmpSet = new HashSet<>();
                for (Object i:
                        (Set) tmp.get(key)) {
                    if (i instanceof Integer) {
                        tmpSet.add((Integer) i);
                    }
                }
                ret.put((Host) key, tmpSet);
            }
        }
        return ret;
    }

    /**
     * Get the log address as LogMetadata of a container.
     * @param containerId The id of the target container.
     * @return The LogMetadata instance store the log address of the given container.
     */
    LogMetadata getLogMetadata(Integer containerId) {
        byte[] data = getData(LOG_METADATA_BASE + getHierarchyPathFromId(containerId));
        return LogMetadata.deserializeLogMetadata(data, containerId);
    }

    /**
     * Create a new instance of the ZKHelper class.
     * @param zkURL The address of this helper instance connect to.
     * @param scope The scope the target stream is in.
     * @param stream The target stream to get status data.
     * @throws ZKConnectionFailedException If cannot connect to the given address.
     * @return The new ZKHelper instance.
     */
    public static ZKHelper create(String zkURL, String scope, String stream) throws ZKConnectionFailedException {
        return new ZKHelper(zkURL, scope, stream);
    }

    /**
     * Get the list of children of a zookeeper node.
     * @param path The path of the target zookeeper node.
     * @return The list of its child nodes' name.
     */
    private List<String> getChild(String path) {
        List<String> ret = null;
        try {
            ret = zkClient.getChildren().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    /**
     * Get the data stored in the zookeeper node.
     * @param path The path of the target zookeeper node.
     * @return The data as byte array stored in the target zookeeper node.
     */
    private byte[] getData(String path) {
        byte[] ret = null;
        try {
            ret = zkClient.getData().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    /**
     * Try to get the cluster name from zookeeper.
     * @return The name of the cluster.
     */
    private String getClusterNameFromZK() {
        List<String> clusterNames = null;
        try {
            clusterNames = zkClient.getChildren().forPath("/");
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (clusterNames == null || clusterNames.size() != 1) {
            ExceptionHandler.NO_CLUSTER.apply();
            return null;
        }

        return clusterNames.get(0);
    }

    /**
     * Get the container's hierarchy path from container id.
     * @param nodeId The container id to get.
     * @return The patch for the container's metadata.
     */
    private String getHierarchyPathFromId(Integer nodeId) {
        StringBuilder pathBuilder = new StringBuilder();
        int value = nodeId;
        for (int i = 0; i < DEFAULT_DEPTH; i++) {
            int r = value % DIVISOR;
            value = value / DIVISOR;
            pathBuilder.append(SEPARATOR).append(r);
        }

        return pathBuilder.append(SEPARATOR).append(nodeId).toString();
    }

    /**
     * Create a curator framework's zookeeper client with given address.
     * @param zkURL The zookeeper address to connect.
     * @throws ZKConnectionFailedException If cannot connect to the zookeeper with given address.
     */
    private void createZKClient(String zkURL) throws ZKConnectionFailedException {
        zkClient = CuratorFrameworkFactory
                .builder()
                .connectString(zkURL)
                .namespace("pravega")
                .retryPolicy(new ExponentialBackoffRetry(2000, 2))
                .build();

        startZKClient();

        clusterName = getClusterNameFromZK();

        zkClient.close();

        zkClient = CuratorFrameworkFactory
                .builder()
                .connectString(zkURL)
                .namespace("pravega/" + clusterName)
                .retryPolicy(new ExponentialBackoffRetry(2000, 2))
                .build();

        startZKClient();
    }

    /**
     * Start the zookeeper client.
     * @throws ZKConnectionFailedException If cannot connect to the zookeeper wothc given address.
     */
    private void startZKClient() throws ZKConnectionFailedException {
        zkClient.start();
        try {
            if (!zkClient.blockUntilConnected(10, TimeUnit.SECONDS)) {
                throw new ZKConnectionFailedException();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Check if the target stream exists. If not, throw the exception using {@link ExceptionHandler} class.
     */
    private void checkStreamExist() {
        try {

            zkClient.getChildren().forPath(streamPath);

        } catch (Exception e) {
            if (e instanceof KeeperException.NoNodeException) {
                ExceptionHandler.NO_STREAM.apply();
            }
        }
    }

    /**
     * Close the ZKHelper.
     */
    void close() {
        zkClient.close();
    }

}
