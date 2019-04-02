/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegacli.commands.utils;

import io.pravega.common.cluster.Host;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Zookeeper helper functions.
 */
public class ZKHelper implements AutoCloseable {

    // region constants

    private static final String BASE_NAMESPACE = "pravega";

    public static final String BK_PATH = "/bookkeeper/ledgers/available";

    private static final String CONTROLLER_PATH = "/cluster/controllers";
    private static final String SEGMENTSTORE_PATH = "/cluster/hosts";
    private static final String HOST_MAP_PATH = "/cluster/segmentContainerHostMapping";
    private static final String STREAM_PATH = "/store/%s/%s";
    private static final String SEGMENT_PATH = STREAM_PATH + "/segment";
    private static final String LOG_METADATA_BASE = "/segmentstore/containers";
    private static final int DEFAULT_DEPTH = 2;
    private static final int DIVISOR = 10;
    private static final String SEPARATOR = "/";

    // endregion

    // region instance variables

    private CuratorFramework zkClient;
    private String clusterName;

    // endregion

    // region constructor

    /**
     * Create a new instance of the ZKHelper class.
     * @param zkURL The address of this helper instance connect to.
     * @throws ZKConnectionFailedException If cannot connect to the given address.
     */
    private ZKHelper(String zkURL) throws ZKConnectionFailedException {
        createZKClient(zkURL);
    }

    // endregion

    /**
     * Get the cluster name.
     * @return The name of the cluster.
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * Get the list of controllers in the cluster.
     * @return A list of controllers.
     */
    public List<String> getControllers() {
        return getChild(CONTROLLER_PATH);
    }

    /**
     * Get the list of segment stores in the cluster.
     * @return A list of segment stores.
     */
    public List<String> getSegmentStores() {
        return getChild(SEGMENTSTORE_PATH);
    }

    /**
     * Get the list of bookies in the cluster.
     * @return A list of bookies.
     */
    public List<String> getBookies() {
        return getChild(BK_PATH);
    }

    /**
     * Get the host to container map.
     * @return A map from segment store host to containers it holds.
     */
    public Map<Host, Set<Integer>> getCurrentHostMap() {
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
     * Get the host with given container.
     * @param containerId The target container's id.
     * @return Host of the container.
     */
    public Optional<Host> getHostForContainer(int containerId) {
        Map<Host, Set<Integer>> mapping = getCurrentHostMap();
        return mapping.entrySet().stream()
                                 .filter(x -> x.getValue().contains(containerId))
                                 .map(Map.Entry::getKey)
                                 .findAny();

    }

    /**
     * Get the log address as LogMetadata of a container.
     * @param containerId The id of the target container.
     * @return The LogMetadata instance store the log address of the given container.
     */
    /*LogMetadata getLogMetadata(Integer containerId) {
        byte[] data = getData(LOG_METADATA_BASE + getHierarchyPathFromId(containerId));
        return LogMetadata.deserializeLogMetadata(data, containerId);
    }*/

    /**
     * Create a new instance of the ZKHelper class.
     * @param zkURL The address of this helper instance connect to.
     * @throws ZKConnectionFailedException If cannot connect to the given address.
     * @return The new ZKHelper instance.
     */
    public static ZKHelper create(String zkURL) throws ZKConnectionFailedException {
        return new ZKHelper(zkURL);
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
                .namespace(BASE_NAMESPACE)
                .retryPolicy(new ExponentialBackoffRetry(2000, 2))
                .build();

        startZKClient();

        clusterName = getClusterNameFromZK();

        zkClient.close();

        zkClient = CuratorFrameworkFactory
                .builder()
                .connectString(zkURL)
                .namespace(BASE_NAMESPACE + SEPARATOR + clusterName)
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
     * Close the ZKHelper.
     */
    public void close() {
        zkClient.close();
    }
}
