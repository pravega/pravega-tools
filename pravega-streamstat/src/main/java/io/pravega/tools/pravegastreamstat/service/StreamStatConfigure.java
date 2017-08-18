/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegastreamstat.service;

import lombok.Getter;
import lombok.Setter;

/**
 * Configuration for the main service.
 * Stores the information about what to print in the service.
 */
@Setter
@Getter
public class StreamStatConfigure {
    private String scope;
    private String stream;
    private String zkURL;
    private String hdfsURL;
    private String hdfsRoot;
    private int offset;

    // region flags

    private boolean data = false;
    private boolean simple = true;
    private boolean cluster = false;
    private boolean txn = false;
    private boolean explicit = false;
    private boolean log = false;
    private boolean recover = false;
    // endregion
}