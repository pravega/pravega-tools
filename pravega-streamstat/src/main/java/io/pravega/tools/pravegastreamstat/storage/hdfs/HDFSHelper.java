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

import io.pravega.tools.pravegastreamstat.logs.operations.AttributeSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.tools.pravegastreamstat.service.MetadataCollector;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import io.pravega.tools.pravegastreamstat.record.DataRecord;
import io.pravega.tools.pravegastreamstat.record.SegmentState;
import io.pravega.tools.pravegastreamstat.service.EventDataAnalyzer;
import io.pravega.tools.pravegastreamstat.service.PrintHelper;
import io.pravega.tools.pravegastreamstat.service.SerializationException;
import io.pravega.tools.pravegastreamstat.service.StreamStatConfigure;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Helper functions for hdfs.
 */
class HDFSHelper implements AutoCloseable {
    private static final String PART_SEPARATOR = "_";


    private static final String NUMBER_GLOB_REGEX = "[0-9]*";
    private static final String NAME_FORMAT = "%s" + PART_SEPARATOR + "%s" + PART_SEPARATOR + "%s";
    private static final String EXAMPLE_NAME_FORMAT = String.format(NAME_FORMAT, "<segment-name>", "<offset>", "<epoch>");
    private static final String STATE_SUFFIX = "$state";
    private static final byte EXPECTED_VERSION = 0;

    private static final int READ_LENGTH = 0xffffff;

    private String hdfsRoot;
    private FileSystem fs;

    /**
     * Create a new instance of this HDFSHelper.
     * @param streamStatConfigure The configure to use in this instance.
     * @throws IOException If Filesystem throws an IOException.
     */
    HDFSHelper(StreamStatConfigure streamStatConfigure) throws IOException {
        Configuration conf = new Configuration();
        String wholeURL = String.format("hdfs://%s/", streamStatConfigure.getHdfsURL());
        conf.set("fs.defaultFS", wholeURL);
        conf.set("fs.default.fs", wholeURL);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        hdfsRoot = streamStatConfigure.getHdfsRoot();
        fs = FileSystem.get(conf);
    }

    // region public API

    /**
     * Print all the data in segment manually.
     * @param segmentName The segment name to print
     * @param data Determine whether to print all the data or just the number of data.
     * @throws HDFSException throw when unable to load HDFS file
     * @throws SerializationException throw when unable to deserialize data
     */
    void printAll(String segmentName, boolean data) throws HDFSException, SerializationException {
        List<FileDescriptor> files;
        try {
            files = findAll(segmentName);
        } catch (IOException e) {
            throw new HDFSException("HDFS Storage", "Cannot find files.", true);
        }
        HDFSSegmentHandle handle = HDFSSegmentHandle.read(segmentName, files);
        handle.checkInvariants();
        handle.print();

        int currentIndex = 0, totalEvents = 0;
        long length = handle.getLength();
        int readLength = READ_LENGTH;

        MetadataCollector.getInstance().getSegmentLength().put(segmentName, length);

        while (currentIndex < length) {
            byte[] buffer = new byte[readLength];
            AtomicInteger byteRead = new AtomicInteger();

            HDFSRead read = new HDFSRead(currentIndex, buffer, readLength, fs);

            PrintHelper.processStart("Reading HDFS files");

            try {
                read.read(handle, byteRead);
            } catch (IOException e) {
                throw new HDFSException("HDFS Storage", "Data not consistent", false);
            }

            PrintHelper.processEnd();

            PrintHelper.processStart("Analyzing data");

            ByteArraySegment byteArray = new ByteArraySegment(buffer);
            ByteArraySegment subSegment = byteArray.subSegment(0, byteRead.get());

            DataRecord dataRecord = EventDataAnalyzer.getDataRecord(subSegment.array(), data);

            currentIndex += dataRecord.getLength();

            // if after an operation we can still read nothing, but there are still data in buffer not handled, means that
            // some data might have been lost so the last part cannot be a complete event.
            if (dataRecord.getLength() == 0) {
                throw new HDFSException("HDFS Storage", "Data not consistent, some byte maybe lost.", false);
            }

            PrintHelper.processEnd();

            if (data) {
                PrintHelper.print(dataRecord.getToPrint());
            } else {
                totalEvents += dataRecord.getEvents();
            }
        }

        if (!data) {
            PrintHelper.printHead("HDFS data");
            PrintHelper.print("DataBytes", length, false);
            PrintHelper.print("EventNumber", totalEvents, true);
        }

        PrintHelper.println();

    }

    /**
     * Print the state of the given segment.
     * @param segmentName           The name of the segment to print.
     * @param segmentNameToIdMap    If found segment id, update this in this map.
     * @throws HDFSException        If cannot connect to HDFS or cannot find the segment state file.
     */
    void printSegmentState(String segmentName, Map<String, Long> segmentNameToIdMap) throws HDFSException {

        PrintHelper.processStart("Reading HDFS stream segment state");

        byte[] stateData;
        try {
            stateData = findStateData(segmentName);
        } catch (IOException e) {
            throw new HDFSException("HDFS Storage Segment State", "Cannot find segment state", true);
        }

        SegmentState state;
        try {
            state = HDFSHelper.deserializeState(stateData);
        } catch (SerializationException e) {
            throw new HDFSException("HDFS Storage Segment State", "Unable to serialize segment state", false);
        }

        PrintHelper.processEnd();

        state.print();
        if (state.getSegmentId() != Long.MIN_VALUE) {
            segmentNameToIdMap.put(state.getSegmentName(), state.getSegmentId());
        }
    }

    /**
     * Get all the segment of a stream from HDFS.
     * @param streamName The stream to get.
     * @return A set of segments that are in the stream.
     */
    Set<String> getSegments(String streamName) {
        Set<String> segmentNames = new HashSet<>();
        try {
            String pattern = streamName + Path.SEPARATOR + NUMBER_GLOB_REGEX;
            val fileStatuses = findAllRaw(pattern);

            for (val fileStatus:
                 fileStatuses) {
                String fileName = fileStatus.getPath().toString();

                int rootSeparator = hdfsRoot.split("/").length - 1;
                String[] fileNameParts = fileName.split("/");
                if (fileNameParts.length < 6 + rootSeparator) {
                    continue;
                }

                String fileNameNoHost = fileNameParts[3 +rootSeparator] +
                        "/" + fileNameParts[4 + rootSeparator] + "/" + fileNameParts[5 + rootSeparator];

                if (fileNameNoHost.contains(STATE_SUFFIX)) {
                    String segmentName = fileNameNoHost.split("[$]")[0];
                    segmentNames.add(segmentName);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return segmentNames;
    }
    // endregion

    // region private utility

    /**
     * Gets an ordered list of FileDescriptors currently available for the given Segment, and validates that they are consistent.
     *
     * @param segmentName      The name of the Segment to retrieve for.
     * @return A List of FileDescriptor
     * @throws IOException If an exception occurred.
     */
    private List<FileDescriptor> findAll(String segmentName) throws IOException {
        FileStatus[] rawFiles = findAllRaw(segmentName);
        if (rawFiles == null || rawFiles.length == 0) {
            throw new FileAlreadyExistsException(segmentName);
        }

        val result = Arrays.stream(rawFiles)
                .map(this::toDescriptor)
                .sorted(this::compareFileDescriptors)
                .collect(Collectors.toList());

        // Validate the names are consistent with the file lengths.
        long expectedOffset = 0;
        for (FileDescriptor fi : result) {
            if (fi.getOffset() != expectedOffset) {
                throw new SegmentFilesCorruptedException(segmentName, fi,
                        String.format("Declared offset is '%d' but should be '%d'.", fi.getOffset(), expectedOffset));
            }

            expectedOffset += fi.getLength();
        }

        return result;
    }

    /**
     * Get the state data as byte array of a given segment.
     * @param segmentName The target segment name.
     * @return The byte array of data that holds the segment state.
     * @throws IOException If the file system throws one.
     */
    private byte[] findStateData(String segmentName) throws IOException {
        List<FileDescriptor> stateFiles =  findAll(segmentName + STATE_SUFFIX);
        assert stateFiles.size() == 1 : "State file not found";

        FileDescriptor stateFile = stateFiles.get(0);

        int length = (int) stateFile.getLength();
        byte[] buffer = new byte[length];
        try (FSDataInputStream stream = this.fs.open(stateFile.getPath())) {
            stream.readFully(0, buffer, 0, length);
        }
        return buffer;
    }

    /**
     * Deserialize state from byte array
     * @param data resource
     * @return segment state
     * @throws SerializationException throw when cannot deserialize.
     */
    private static SegmentState deserializeState(byte[] data) throws SerializationException {
        ByteArraySegment byteArraySegment = new ByteArraySegment(data);
        InputStream inputStream = byteArraySegment.getReader();
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        try  {
            byte version = dataInputStream.readByte();
            if (version != EXPECTED_VERSION) {
                throw new SerializationException("Segment state", "Segment state version unknown");
            }
            long segmentId = dataInputStream.readLong();
            String segmentName = dataInputStream.readUTF();

            Map<UUID, Long> attributes = AttributeSerializer.deserialize(dataInputStream);

            return new SegmentState(version, segmentId, segmentName, attributes);
        } catch (IOException e) {
            throw new SerializationException("Segment state", "Unable to deserialize");
        }

    }

    /**
     * Get the segment path with HDFSRoot.
     * @param segmentName The target segment name to get path.
     * @return A path with HDFSRoot prefix.
     */
    private String getPathPrefix(String segmentName) {
        return hdfsRoot + Path.SEPARATOR + segmentName;
    }

    /**
     * Find the data of the whole segment as File status.
     * @param segmentName Target segment to find data.
     * @return An array of file status of the segment.
     * @throws IOException If the file system throws one.
     */
    private FileStatus[] findAllRaw(String segmentName) throws IOException {
        assert segmentName != null && segmentName.length() > 0 : "segmentName must be non-null and non-empty";
        String pattern = String.format(NAME_FORMAT, getPathPrefix(segmentName), NUMBER_GLOB_REGEX, NUMBER_GLOB_REGEX);

        return this.fs.globStatus(new Path(pattern));
    }


    /**
     * Converts the given FileStatus into a FileDescriptor.
     * @param fs FileStatus to convert.
     * @return The FileDescriptor got.
     */
    @SneakyThrows(FileNameFormatException.class)
    private FileDescriptor toDescriptor(FileStatus fs) {
        // Extract offset and epoch from name.
        final long offset;
        final long epoch;
        String fileName = fs.getPath().getName();

        // We read backwards, because the segment name itself may have multiple PartSeparators in it, but we only care
        // about the last ones.
        int pos2 = fileName.lastIndexOf(PART_SEPARATOR);
        if (pos2 <= 0 || pos2 >= fileName.length() - 1) {
            throw new FileNameFormatException(fileName, "File must be in the following format: " + EXAMPLE_NAME_FORMAT);
        }

        int pos1 = fileName.lastIndexOf(PART_SEPARATOR, pos2 - 1);
        if (pos1 <= 0 || pos1 >= fileName.length() - 1) {
            throw new FileNameFormatException(fileName, "File must be in the following format: " + EXAMPLE_NAME_FORMAT);
        }

        try {
            offset = Long.parseLong(fileName.substring(pos1 + 1, pos2));
            epoch = Long.parseLong(fileName.substring(pos2 + 1));
        } catch (NumberFormatException nfe) {
            throw new FileNameFormatException(fileName, "Could not extract offset or epoch.", nfe);
        }

        return new FileDescriptor(fs.getPath(), offset, fs.getLen(), epoch, isReadOnly(fs));
    }

    private boolean isReadOnly(FileStatus fs) {
        return fs.getPermission().getUserAction() == FsAction.READ;
    }

    private int compareFileDescriptors(FileDescriptor f1, FileDescriptor f2) {
        int diff = Long.compare(f1.getOffset(), f2.getOffset());
        if (diff == 0) {
            diff = Long.compare(f1.getEpoch(), f2.getEpoch());
        }

        return diff;
    }

    // endregion

    @Override
    public void close() throws IOException {
        fs.close();
    }

}
