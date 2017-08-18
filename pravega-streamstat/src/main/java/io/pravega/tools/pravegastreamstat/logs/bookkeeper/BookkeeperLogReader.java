/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegastreamstat.logs.bookkeeper;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import io.pravega.tools.pravegastreamstat.logs.LogReader;
import io.pravega.tools.pravegastreamstat.service.PrintHelper;

/**
 * Performs read from BookKeeper Logs.
 */

@RequiredArgsConstructor
public class BookkeeperLogReader implements LogReader {

    //region Members

    private final BookKeeper bookKeeper;
    private final LogMetadata metadata;
    private final long explicit;
    private ReadLedger currentLedger;

    //endregion

    //region CloseableIterator Implementation

    @Override
    public ReadItem getNext() {

        if (this.currentLedger == null) {
            // First time we call this. Locate the first ledger based on the metadata truncation address. We don't know
            // how many entries are in that first ledger, so open it anyway so we can figure out.
            openNextLedger(this.metadata.getNextAddress(this.metadata.getTruncationAddress(), Long.MAX_VALUE));
        }

        while (this.currentLedger != null && (!this.currentLedger.canRead())) {
            // We have reached the end of the current ledger. Find next one, and skip over empty ledgers).
            val lastAddress = new LedgerAddress(this.currentLedger.metadata, this.currentLedger.handle.getLastAddConfirmed() + explicit);
            try {
                this.currentLedger.handle.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            LedgerAddress nextAddress = this.metadata.getNextAddress(lastAddress, this.currentLedger.handle.getLastAddConfirmed() + explicit);
            if (nextAddress == null) {
                return null;
            }

            openNextLedger(nextAddress);
        }

        // Try to read from the current reader.
        if (this.currentLedger == null || this.currentLedger.reader == null) {
            return null;
        }

        val e = currentLedger.reader.nextElement();

        return new BookkeeperLogReader.ReadItem(e, this.currentLedger.metadata);
    }

    @Override
    @SneakyThrows
    public void close() {
        this.currentLedger.handle.close();
    }

    @SneakyThrows
    private void openNextLedger(LedgerAddress address) {
        if (address == null) {
            // We have reached the end.
            return;
        }

        LedgerMetadata metadata = new LedgerMetadata(address.getLedgerId(), address.getLedgerSequence());

        // Open the ledger.
        LedgerHandle ledger = bookKeeper.openLedgerNoRecovery(metadata.getLedgerId(), BookKeeper.DigestType.MAC, "".getBytes());

        long lastEntryId = ledger.getLastAddConfirmed() + explicit;
        if (lastEntryId < address.getEntryId()) {
            // This ledger is empty.
            ledger.close();
            this.currentLedger = new ReadLedger(metadata, ledger, null);
            return;
        }

        try {
            val reader = ledger.readEntries(address.getEntryId(), lastEntryId);
            this.currentLedger = new ReadLedger(metadata, ledger, reader);
        } catch (Exception ex) {
            if (ex instanceof BKException.BKReadException && explicit > 0) {
                PrintHelper.processStart("Waiting for the last log available");
                while (true) {

                    // reopen the ledger to check if there is new readable
                    ledger.close();
                    ledger = bookKeeper.openLedgerNoRecovery(metadata.getLedgerId(), BookKeeper.DigestType.MAC, "".getBytes());

                    // check if LAC changed
                    if (ledger.getLastAddConfirmed() >= lastEntryId) {
                        val reader = ledger.readEntries(address.getEntryId(), lastEntryId);
                        this.currentLedger = new ReadLedger(metadata, ledger, reader);
                        break;
                    } else {
                        Thread.sleep(1000);
                    }
                }

                PrintHelper.processEnd();
            } else {
                ledger.close();
            }
        }
    }

    //endregion

    //region ReadItem
    public static class ReadItem implements LogReader.ReadItem {
        @Getter
        private final InputStream payload;
        @Getter
        private final int length;
        @Getter
        private final LedgerAddress address;

        @SneakyThrows(IOException.class)
        ReadItem(LedgerEntry entry, LedgerMetadata ledgerMetadata) {
            this.address = new LedgerAddress(ledgerMetadata, entry.getEntryId());
            this.payload = entry.getEntryInputStream();
            this.length = this.payload.available();
        }

        @Override
        public String toString() {
            return String.format("%s, Length = %d.", this.address, this.length);
        }
    }

    //endregion

    //region ReadLedger

    @RequiredArgsConstructor
    private static class ReadLedger {
        final LedgerMetadata metadata;
        final LedgerHandle handle;
        final Enumeration<LedgerEntry> reader;

        boolean canRead() {
            return this.reader != null && this.reader.hasMoreElements();
        }
    }

    //endregion
}

