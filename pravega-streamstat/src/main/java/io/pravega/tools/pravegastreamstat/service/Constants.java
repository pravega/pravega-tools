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

/**
 * Constants for main service.
 */
class Constants {
    static final String DEFAULT_ZK_URL = "localhost:2181";
    static final int DEFAULT_CONTAINER_COUNT = 4;

    static final String DEFAULT_STREAM_NAME = "examples/someStream";

    static final String DEFAULT_HDFS_URL = "localhost:8020";
    static final String DEFAULT_HDFS_ROOT = "";
}