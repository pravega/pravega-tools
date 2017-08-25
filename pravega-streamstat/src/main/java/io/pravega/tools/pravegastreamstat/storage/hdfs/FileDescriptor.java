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

import lombok.Getter;

import org.apache.hadoop.fs.Path;
import org.apache.http.annotation.GuardedBy;
import org.apache.http.annotation.ThreadSafe;

/**
 * File descriptor for a segment file.
 */
@ThreadSafe
public class FileDescriptor {
    // region Members
    /**
     * The full HDFS path to this file.
     */
    @Getter
    private final Path path;

    /**
     * Segment offset of the first byte of this file. This is derived from the name.
     */
    @Getter
    private final long offset;

    /**
     * Epoch when the file was created. This is derived from the name.
     */
    @Getter
    private final long epoch;

    @GuardedBy("this")
    private long length;
    @GuardedBy("this")
    private boolean readOnly;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the FileDescriptor class.
     *
     * @param path     The path of the file.
     * @param offset   The Segment Offset of the first byte in the file.
     * @param length   The length of the file.
     * @param epoch    The epoch the file was created in.
     * @param readOnly Whether the file is read-only.
     */
    public FileDescriptor(Path path, long offset, long length, long epoch, boolean readOnly) {
        this.path = path;
        this.offset = offset;
        this.length = length;
        this.epoch = epoch;
        this.readOnly = readOnly;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the length of this file. Invocations of
     *
     * @return The length of this file.
     */
    public synchronized long getLength() {
        return this.length;
    }

    /**
     * Gets a value indicating the Segment offset corresponding to the last byte in this file.
     *
     * @return The result.
     */
    synchronized long getLastOffset() {
        return this.offset + this.length;
    }

    @Override
    public synchronized String toString() {
        return String.format("%s (%d, %s)", this.path.getName(), this.length, this.readOnly ? "R" : "RW");
    }

    //endregion
}