/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli;

import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;
import io.pravega.test.integration.utils.SetupUtils;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import org.junit.*;
import org.junit.rules.Timeout;

import java.io.ByteArrayOutputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.micrometer.shaded.reactor.core.publisher.Mono.when;
import static org.mockito.Mockito.mock;

/**
 * Validate basic troubleshoot commands
 */
public class TroubleshootCommandsTest {
    // Setup utility.
    private static final SetupUtils SETUP_UTILS = new SetupUtils();
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    @Rule
    public final Timeout globalTimeout = new Timeout(360, TimeUnit.SECONDS);

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.startAllServices();
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controllerRestUri", SETUP_UTILS.getControllerRestUri().toString());
        pravegaProperties.setProperty("pravegaservice.zkURL", "localhost:2181");
        pravegaProperties.setProperty("pravegaservice.containerCount", "4");
        STATE.get().getConfigBuilder().include(pravegaProperties);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void generalCheckTest() throws Exception {

    }

    @Test
    public void testUpdateCheck() throws Exception {
        String testStream = "testStream";
        SETUP_UTILS.createTestStream(testStream, 1);
        String commandResult = TestUtils.executeCommand("troubleshoot update-check " + SETUP_UTILS.getScope(), STATE.get());
        Assert.assertTrue(commandResult.contains());
    }

    @Test
    public void scaleCheckTest() throws Exception {

    }

    @Test
    public void committingTransactionsCheckTest() throws Exception {

    }

    @Test
    public void truncateCheckTest() throws Exception {

    }
}
