/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli.commands.utils;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

public final class CLIControllerConfig {

    public static final Property<String> CONTROLLER_REST_URI = Property.named("controllerRestUri", "http://localhost:9091");
    public static final Property<String> CONTROLLER_USER_NAME = Property.named("userName", "");
    public static final Property<String> CONTROLLER_PASSWORD = Property.named("password", "");

    public static final String COMPONENT_CODE = "cli";

    /**
     * The Controller REST URI. Recall to set "http" or "https" depending on the TLS configuration of the Controller.
     */
    @Getter
    private final String controllerRestURI;

    /**
     * User name if authentication is configured in the Controller.
     */
    @Getter
    private final String userName;

    /**
     * Password if authentication is configured in the Controller.
     */
    @Getter
    private final String password;

    private CLIControllerConfig(TypedProperties properties) throws ConfigurationException {
        this.controllerRestURI = properties.get(CONTROLLER_REST_URI);
        this.userName = properties.get(CONTROLLER_USER_NAME);
        this.password = properties.get(CONTROLLER_PASSWORD);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<CLIControllerConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, CLIControllerConfig::new);
    }
}
