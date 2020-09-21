/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.test.common.SecurityConfigDefaults;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

public class AuthEnabledControllerDescribeStreamCommandTest extends ControllerDescribeStreamCommandTest{
    @Before
    public void setUp() throws Exception {
        this.AUTH_ENABLED = true;
        super.setUp();
    }

    @Override
    ClientConfig prepareValidClientConfig() {
        return ClientConfig.builder()
                .controllerURI(URI.create(this.localPravega.getInProcPravegaCluster().getControllerURI()))

                // Auth-related
                .credentials(new DefaultCredentials(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD,
                        SecurityConfigDefaults.AUTH_ADMIN_USERNAME))
                .build();
    }

    @Test
    @Override
    public void testDescribeStreamCommand() throws Exception {
        super.testDescribeStreamCommand();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }
}
