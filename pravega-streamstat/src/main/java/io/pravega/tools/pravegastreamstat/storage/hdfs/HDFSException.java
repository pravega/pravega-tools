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

/**
 *  An exception that is thrown when cannot find segment state file, or some file of with some expected
 *  offset of a segment cannot be found, which indicate that some file of the segment might be lost.
 */
public class HDFSException extends Exception {
    public boolean isMissingFile;
    public HDFSException(String src, String msg, boolean isMissingFile) {
        super(String.format("%s: %s", src, msg));
        this.isMissingFile = isMissingFile;
    }
}
