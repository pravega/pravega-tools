/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegastreamstat.record;

import lombok.Data;

/**
 * Stores the event data and its info.
 */
@Data
public class DataRecord {
    final String toPrint;
    final int events;
    final long length;
}
