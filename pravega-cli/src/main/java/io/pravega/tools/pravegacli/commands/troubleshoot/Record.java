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

import lombok.Builder;

import java.lang.reflect.Type;

public class Record<T> {
    private final T record;
    private final Type recordType;

    @Builder
    public Record(T record, Type recordType) {
        this.record = record;
        this.recordType = recordType;
    }
}
