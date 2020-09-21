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

import io.pravega.test.integration.utils.SetupUtils;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Validate basic controller commands.
 */
public class ControllerCommandsTest {
    // Setup utility.
    private static final SetupUtils SETUP_UTILS = new SetupUtils();
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    @Rule
    public final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.startAllServices();
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();

        // The uri returned by SETUP_UTILS is in the form http://localhost:9091 (protocol + domain + port)
        // but for the CLI we need to set the REST uri as localhost:9091 (domain + port). Because the protocol
        // is decided based on whether security is enabled or not.
        pravegaProperties.setProperty("cli.controllerRestUri", SETUP_UTILS.getControllerRestUri().toString().substring(7));
        pravegaProperties.setProperty("pravegaservice.zkURL", "localhost:2181");
        pravegaProperties.setProperty("pravegaservice.containerCount", "4");
        pravegaProperties.setProperty("cli.authEnabled", "false");
        pravegaProperties.setProperty("cli.tlsEnabled", "false");
        STATE.get().getConfigBuilder().include(pravegaProperties);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testListScopesCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("controller list-scopes", STATE.get());
        Assert.assertTrue(commandResult.contains("_system"));
    }

    @Test
    public void testListStreamsCommand() throws Exception {
        String testStream = "testStream";
        SETUP_UTILS.createTestStream(testStream, 1);
        String commandResult = TestUtils.executeCommand("controller list-streams " + SETUP_UTILS.getScope(), STATE.get());
        Assert.assertTrue(commandResult.contains(testStream));
    }

    @Test
    public void testListReaderGroupsCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("controller list-readergroups _system", STATE.get());
        Assert.assertTrue(commandResult.contains("commitStreamReaders"));
    }

    @Test
    public void testDescribeScopeCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("controller describe-scope _system", STATE.get());
        Assert.assertTrue(commandResult.contains("_system"));
    }

    @Test
    public void testDescribeReaderGroupCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("controller describe-readergroup _system scaleGroup", STATE.get());
        Assert.assertTrue(commandResult.contains("scaleGroup"));
    }
}
