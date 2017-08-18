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

import io.pravega.common.Exceptions;

import java.util.List;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.http.annotation.GuardedBy;
import org.apache.http.annotation.ThreadSafe;
import io.pravega.tools.pravegastreamstat.service.ExceptionHandler;
import io.pravega.tools.pravegastreamstat.service.PrintHelper;

/**
 * Base Handle for HDFSStorage.
 */
@ThreadSafe
public class HDFSSegmentHandle {
    //region Members

    @Getter
    private final String segmentName;
    @Getter
    private final boolean readOnly;
    @GuardedBy("files")
    @Getter
    private final List<FileDescriptor> files;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSSegmentHandle class.
     *
     * @param segmentName The name of the Segment in this Handle, as perceived by users of the Segment interface.
     * @param readOnly    Whether this handle is read-only or not.
     * @param files       A ordered list of initial files for this handle.
     */
    private HDFSSegmentHandle(String segmentName, boolean readOnly, List<FileDescriptor> files) {
        Exceptions.checkNotNullOrEmpty(segmentName, "segmentName");
        Exceptions.checkNotNullOrEmpty(files, "files");

        this.segmentName = segmentName;
        this.readOnly = readOnly;
        this.files = files;
    }

    /**
     * Creates a read-only handle.
     *
     * @param segmentName The name of the Segment to create the handle for.
     * @param files       A ordered list of initial files for this handle.
     * @return The new handle.
     */
    public static HDFSSegmentHandle read(String segmentName, List<FileDescriptor> files) {
        return new HDFSSegmentHandle(segmentName, true, files);
    }

    //endregion

    //region print

    public void print() {
        PrintHelper.printHead("HDFS Storage segment");
        PrintHelper.println();
        PrintHelper.print("Storage Epoch", getEpoch(), false);

        String fileNames;
        synchronized (this.files) {
            fileNames = StringUtils.join(this.files, ", ");
        }

        PrintHelper.print(String.format("(Files: %s)", fileNames));

        PrintHelper.println();
        PrintHelper.println();
    }

    private long getEpoch() {
        return getLastFile().getEpoch();
    }

    /**
     * Gets the last file in the file list for this handle.
     * @return The FileDescriptor for the last file.
     */
    private FileDescriptor getLastFile() {
        synchronized (this.files) {
            return this.files.get(this.files.size() - 1);
        }
    }

    /**
     * Get the total length.
     * @return the length of the whole segment.
     */
    public long getLength() {
        return files.stream().mapToLong(FileDescriptor::getLength).sum();
    }

    //endregion

    public void checkInvariants() {
        long start = Long.MAX_VALUE, end = 0;

        for (FileDescriptor fd:
                files) {
            start = fd.getOffset() < start ? fd.getOffset() : start;
            end = fd.getLastOffset() > end ? fd.getLastOffset() : end;
        }

        if (start != 0) {
            ExceptionHandler.T2_OFFSET_NOT_START_ZERO.apply();
        }

        if (end - start != getLength()) {
            ExceptionHandler.DATA_NOT_CONTINUOUS.apply();
        }
    }
}
