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

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Exception types and printers.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum ExceptionHandler {

    // region input errors
    INVALID_STREAM_NAME("INVALID STREAM NAME", false),
    INVALID_OFFSET_NUMBER("INVALID OFFSET NUMBER", false),
    CANNOT_READ_STREAM_NAME("CANNOT READ STREAM NAME", false),
    // endregion

    // region HDFS errors
    HDFS_CANNOT_CONNECT("Cannot connect to HDFS, please check the HDFS url given.", true),

    // endregion

    // region zk errors
    NO_ZK("Cannot establish connection to zookeeper, " +
            "will only print data and possible metadata from storage.", false),
    NO_STREAM("No such stream", false),
    NO_CLUSTER("Cannot find pravega cluster in zookeeper, please check zookeeper url.", false),

    // endregion

    // region invariants not hold.
    NO_SEGMENT_ID("Cannot find segment ID in both tier.", true),
    NO_TXN("NO transaction found.", true),
    METADATA_NOT_CONSISTENT("The data found in storage does not match the metadata found in zookeeper, use -r to recover metadata", false),
    DATA_NOT_CONTINUOUS("Data is not consistent in tier 1 log, some log might be lost.", true),
    T2_OFFSET_NOT_START_ZERO("Tier-2 offset not start with zero", true),
    T1_OFFSET_LOWER_THAN_T2("Tier-1 largest offset is smaller than tier-2.", true);
    // endregion

    private static final List<ExceptionHandler> EXCEPTION_HANDLERS = new ArrayList<>();

    final String msg;
    final boolean isWarning;

    /**
     * Print the exception and add them to the exception list.
     */
    public void apply() {
        PrintHelper.printError(msg);
        if (!isWarning) {
            System.exit(-1);
        }

        EXCEPTION_HANDLERS.add(this);
    }

    /**
     * Print out all the exceptions found.
     */
    public static void summarize() {
        if (EXCEPTION_HANDLERS.isEmpty()) {
            PrintHelper.println(PrintHelper.Color.GREEN, "No errors found");
            return;
        }

        PrintHelper.printHead("These issues are found");
        for (int i = 0; i < EXCEPTION_HANDLERS.size(); i++ ) {
            ExceptionHandler e = EXCEPTION_HANDLERS.get(i);
            PrintHelper.print(e.toString(), e.msg, i == EXCEPTION_HANDLERS.size()-1);
        }
    }
}
