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

import io.pravega.common.util.CollectionHelpers;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * mock io.pravega.segmentstore.storage.impl.bookkeeper.BookKeepers.LogMetadata.
 */
@RequiredArgsConstructor
@Data
public class LogMetadata {
    private final long epoch;
    private final List<LedgerMetadata> ledgers;
    private final LedgerAddress truncationAddress;
    private final Integer containerId;

    public static LogMetadata deserializeLogMetadata(byte[] serialization, int containerId) {
        ByteBuffer bb = ByteBuffer.wrap(serialization);
        bb.get(); // We skip version for now because we only have one.
        long epoch = bb.getLong();

        // Truncation Address.
        long truncationSeqNo = bb.getLong();
        long truncationLedgerId = bb.getLong();

        // Ledgers
        int ledgerCount = bb.getInt();
        List<LedgerMetadata> ledgers = new ArrayList<>(ledgerCount);
        for (int i = 0; i < ledgerCount; i++) {
            long ledgerId = bb.getLong();
            int seq = bb.getInt();
            ledgers.add(new LedgerMetadata(ledgerId, seq));
        }

        return new LogMetadata(epoch, Collections.unmodifiableList(ledgers), new LedgerAddress(truncationSeqNo, truncationLedgerId), containerId);
    }

    LedgerAddress getNextAddress(LedgerAddress address, long lastEntryId) {
        if (this.ledgers.size() == 0) {
            // Quick bail-out. Nothing to return.
            return null;
        }

        LedgerAddress result = null;
        LedgerMetadata firstLedger = this.ledgers.get(0);
        if (address.getLedgerSequence() < firstLedger.getSequence()) {
            // Most likely an old address. The result is the first address of the first ledger we have.
            result = new LedgerAddress(firstLedger, 0);
        } else if (address.getEntryId() < lastEntryId) {
            // Same ledger, next entry.
            result = new LedgerAddress(address.getLedgerSequence(), address.getLedgerId(), address.getEntryId() + 1);
        } else {
            // Next ledger. First try a binary search, hoping the ledger in the address actually exists.
            LedgerMetadata ledgerMetadata = null;
            int index = getLedgerMetadataIndex(address.getLedgerId()) + 1;
            if (index > 0) {
                // Ledger is in the list. Make sure it's not the last one.
                if (index < this.ledgers.size()) {
                    ledgerMetadata = this.ledgers.get(index);
                }
            } else {
                // Ledger was not in the list. We need to find the first ledger with an id larger than the one we have.
                for (LedgerMetadata lm : this.ledgers) {
                    if (lm.getLedgerId() > address.getLedgerId()) {
                        ledgerMetadata = lm;
                        break;
                    }
                }
            }

            if (ledgerMetadata != null) {
                result = new LedgerAddress(ledgerMetadata, 0);
            }
        }

        if (result != null && result.compareTo(this.truncationAddress) < 0) {
            result = this.truncationAddress;
        }

        return result;
    }

    private int getLedgerMetadataIndex(long ledgerId) {
        return CollectionHelpers.binarySearch(this.ledgers, lm -> Long.compare(ledgerId, lm.getLedgerId()));
    }

    @Override
    public String toString() {
        return String.format("epoch = %d, ledgers = %s, truncationAddress = %s",
                epoch, ledgers, truncationAddress);
    }
}
