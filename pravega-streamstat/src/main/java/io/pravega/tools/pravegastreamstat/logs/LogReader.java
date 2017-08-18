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

import io.pravega.common.util.CloseableIterator;

import java.io.InputStream;

/**
 * Perform read from tier-1 log.
 */
public interface LogReader extends CloseableIterator {
    ReadItem getNext();

    interface ReadItem {
        InputStream getPayload();

        int getLength();
    }
}
