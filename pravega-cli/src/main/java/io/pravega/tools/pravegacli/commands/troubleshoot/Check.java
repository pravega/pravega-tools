/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli.commands.troubleshoot;

import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;

import java.util.concurrent.ScheduledExecutorService;

/**
 * An interface for the basic check method
 */
public interface Check {

    /**
     * Method to check the consistency of a given case of records
     *
     * @param store     an instance of the extended metadata store
     * @param executor  callers executor
     * @return A boolean which represents whether there is any error or not.
     */
    boolean check(ExtendedStreamMetadataStore store, ScheduledExecutorService executor);
}
