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

import io.pravega.common.util.CollectionHelpers;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An operation for hdfs read.
 * Each segment is a collection of ordered files, adopting the following pattern: {segment-name}_{start-offset}_{epoch}.
 * <ul>
 *     <li> {segment-name} is the the name of the segment as used in the SegmentStore </li>
 *     <li> {start-offset} is the offset with in the Segment of the first byte in this file. </li>
 *     <li> {epoch} is the Container Epoch which had owner ship of that segment when that file was created. </li>
 * </ul>
 * Example: Segment "foo" can have these files
 * <ol>
 * <li> foo_0_1: first file, contains data at offsets [0..1234), written during Container Epoch 1.
 * <li> foo_1234_2: data at offsets [1234..987632), written during Container Epoch 2.
 * <li> foo_987632_3: data at offsets [987632..10568949), written during Container Epoch 3.
 * <li> foo_10568949_4: data at offsets [10568949..current-length-of-segment), written during Container Epoch 4.
 * </ol>
 *
 * This class will set an operation that can read with the given offset and length, and store the read data in the buffer.
 */
@RequiredArgsConstructor
public class HDFSRead {

    /**
     * The starting offset of the read operation.
     */
    private final long offset;

    /**
     * The buffer to store the result.
     */
    @Getter
    private final byte[] buffer;

    /**
     * The length to read.
     */
    private final int length;

    /**
     * The resource FileSystem.
     */
    private final FileSystem fs;

    /**
     * Ref: io.pravega.segmentstore.storage.impl.hdfs.ReadOperation.read
     * Read all the data from handle, save them to buffer.
     * @param handle the segment handle of all hdfs files
     * @param totalBytesRead record totalBytes read.
     * @throws IOException if unable to read as expected.
     */
    public void read(HDFSSegmentHandle handle, AtomicInteger totalBytesRead) throws IOException {
        val handleFiles = handle.getFiles();
        int currentFileIndex = CollectionHelpers.binarySearch(handleFiles, this::compareToStartOffset);
        assert currentFileIndex >= 0 : "unable to locate first file index.";
        while (totalBytesRead.get() < this.length && currentFileIndex < handleFiles.size()) {
            FileDescriptor currentFile = handleFiles.get(currentFileIndex);
            long fileOffset = this.offset + totalBytesRead.get() - currentFile.getOffset();
            int fileReadLength = (int) Math.min(this.length - totalBytesRead.get(), currentFile.getLength() - fileOffset);
            assert fileOffset >= 0 && fileReadLength >= 0 : "negative file read offset or length";

            try (FSDataInputStream stream = this.fs.open(currentFile.getPath())) {
                stream.readFully(fileOffset, this.buffer, totalBytesRead.get(), fileReadLength);
                totalBytesRead.addAndGet(fileReadLength);
            } catch (EOFException ex) {
                throw new IOException(
                        String.format("Internal error while reading segment file. Attempted to read file '%s' at offset %d, length %d.",
                                currentFile, fileOffset, fileReadLength),
                        ex);
            }

            currentFileIndex++;
        }
    }

    private int compareToStartOffset(FileDescriptor fi) {
        if (this.offset < fi.getOffset()) {
            return -1;
        } else if (this.offset >= fi.getLastOffset()) {
            return 1;
        } else {
            return 0;
        }
    }

}
