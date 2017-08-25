/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegastreamstat.service;

import io.pravega.tools.pravegastreamstat.logs.bookkeeper.LogMetadata;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An singleton class that stores the metadata found in both tier of the storage.
 * Including
 * <h3>Metadata found in tier-2: </h3>
 * <ul>
 *     <li>A list of segments that can be found in tier-2. </li>
 *     <li>A map from segments to its length found in tier-2. </li>
 * </ul>
 *
 * <h3>Metadata found in zookeeper: </h3>
 * <ul>
 *     <li>The cluster's total container account. </li>
 *     <li>The cluster's name. </li>
 *     <li>A map from each segment's name and which container it's in. </li>
 *     <li>A map from each container to it's log address. </li>
 * </ul>
 *
 * <h3>And some other information collected: </h3>
 * <ul>
 *     <li>A map from each segment's name to its segment id. </li>
 * </ul>
 */
@Data
public class MetadataCollector {
    private final static MetadataCollector INSTANCE = new MetadataCollector();

    // region Metadata found in Tier-2 Storage

    /**
     * A list of segments found in tier-2 storage.
     */
    private List<String> segments = new ArrayList<>();

    /**
     * The segment's length found in tier-2 storage.
     */
    private Map<String, Long> segmentLength = new HashMap<>();

    // endregion

    // region Metadata found in Zookeeper

    private Integer containerCount = Constants.DEFAULT_CONTAINER_COUNT;
    private String clusterName;
    private Map<String, Long> segmentNameToIdMap = new HashMap<>();
    private Map<String, Integer> segmentNameToContainerMap = new HashMap<>();
    private Map<Integer, LogMetadata> containerToLogMetadataMap = new HashMap<>();
    private boolean healthZk = true;

    // endregion

    public static MetadataCollector getInstance() {
        return INSTANCE;
    }

}
