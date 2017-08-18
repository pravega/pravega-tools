/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegastreamstat.logs;

import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import lombok.Data;
import lombok.Getter;
import io.pravega.tools.pravegastreamstat.service.PrintHelper;

import java.io.IOException;

/**
 * Data in log
 */

class DataFrame {

    // region static member

    private final static int HEADER_LENGTH = 6;

    // endregion

    // region instance variables

    @Getter
    private byte[] header = new byte[HEADER_LENGTH];
    private ByteArraySegment contents;
    private int currentPosition = 0;

    // endregion

    // region constructor

    /**
     * The constructor to create a data frame from the log reader's read item.
     * @param item the read item given from data frame.
     */
    DataFrame(LogReader.ReadItem item) {
        try {
            if (item.getPayload().read(header) != HEADER_LENGTH) {
                throw new IOException();
            }
            contents = new ByteArraySegment(StreamHelpers.readAll(item.getPayload(), item.getLength() - 6));
        } catch (IOException e) {
            contents = new ByteArraySegment(new byte[0]);
        }
    }

    // endregion

    /**
     * Get an entry from the frame.
     * @return the entry got.
     */
    DataEntry getEntry()  {
        if (currentPosition >= contents.getLength()) {
            return null;
        }

        // Integrity check. This means that we have a corrupt frame.
        if (this.currentPosition + DataEntry.HEADER_SIZE > contents.getLength()) {
            return null;
        }
        // Determine the length of the next record && advance the position by the appropriate amount of bytes.
        DataEntry entry = new DataEntry(this.contents.subSegment(this.currentPosition, DataEntry.HEADER_SIZE));
        this.currentPosition += DataEntry.HEADER_SIZE;

        // Integrity check. This means that we have a corrupt frame.
        if (this.currentPosition + entry.getEntryLength() > contents.getLength() || entry.getEntryLength() < 0) {
            return null;
        }

        // Get the result contents && advance the positions.
        ByteArraySegment resultContents = this.contents.subSegment(this.currentPosition, entry.getEntryLength());
        this.currentPosition += entry.getEntryLength();

        entry.setData(resultContents);
        return entry;
    }

    /**
     * The class that hold an entry from the data frame.
     */
    @Data
    public static class DataEntry {
        private static final int HEADER_SIZE = Integer.BYTES + Byte.BYTES;
        private static final int FLAGS_OFFSET = Integer.BYTES;

        private static final byte FIRST_ENTRY_MASK = 1;
        private static final byte LAST_ENTRY_MASK = 2;

        private Integer entryLength;

        private boolean firstRecordEntry;
        private boolean lastRecordEntry;
        private ByteArraySegment data;

        /**
         * The constructor to create an entry according to the header.
         * @param header The entry header.
         */
        DataEntry(ByteArraySegment header) {
            this.entryLength = BitConverter.readInt(header, 0);
            byte flags = header.get(FLAGS_OFFSET);
            this.firstRecordEntry = (flags & FIRST_ENTRY_MASK) == FIRST_ENTRY_MASK;
            this.lastRecordEntry = (flags & LAST_ENTRY_MASK) == LAST_ENTRY_MASK;
            this.data = null;
        }

        /**
         * Print the information stored in the entry.
         */
        public void print() {
            PrintHelper.print(PrintHelper.Color.CYAN, String.format("DataFrameEntry: entryLength = %d, " +
                            "firstRecordEntry = %b, lastRecordEntry = %b, data = %d",
                    entryLength, firstRecordEntry, lastRecordEntry, data.getLength()));
            PrintHelper.println();

        }
    }



}
