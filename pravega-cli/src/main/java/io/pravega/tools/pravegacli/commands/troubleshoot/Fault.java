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
import lombok.Getter;

/**
 * A class for storing a fault.
 */
public class Fault {

    public enum InconsistencyType {
        UNAVAILABLE,
        INCONSISTENT
    }

    @Getter
    private final InconsistencyType inconsistencyType;
    @Getter
    private final Record inconsistentWith;
    @Getter
    private final String errorMessage;

    @Builder
    private Fault(InconsistencyType inconsistencyType, Record inconsistentWith, String errorMessage) {
        this.inconsistencyType = inconsistencyType;
        this.inconsistentWith = inconsistentWith;
        this.errorMessage = errorMessage;
    }

    /**
     * Constructor for UNAVAILABLE type. Data corruption or absence.
     *
     * @param errorMessage the error message
     * @return a new fault
     */
    public static Fault unavailable(String errorMessage) {
        return new Fault(InconsistencyType.UNAVAILABLE, null, errorMessage);
    }

    /**
     * Constructor for INCONSISTENT type. Data inconsistency or mismatch among records.
     *
     * @param inconsistentWith the record
     * @param errorMessage     the error message
     * @return a new fault
     */
    public static Fault inconsistent(Record inconsistentWith, String errorMessage) {
        return new Fault(InconsistencyType.INCONSISTENT, inconsistentWith, errorMessage);
    }
}
