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
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.controller.ControllerCommand;
import io.pravega.tools.pravegacli.commands.controller.ControllerDescribeScopeCommand;
import io.pravega.tools.pravegacli.commands.controller.ControllerListReaderGroupsInScopeCommand;
import io.pravega.tools.pravegacli.commands.controller.ControllerListScopesCommand;
import io.pravega.tools.pravegacli.commands.controller.ControllerListStreamsInScopeCommand;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class ControllerCommandsTest {
    // Setup utility.
    private static final SetupUtils SETUP_UTILS = new SetupUtils();
    private static AdminCommandState STATE;

    @Rule
    public final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.startAllServices();
        STATE = new AdminCommandState();
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controllerRestUri", SETUP_UTILS.getControllerRestUri().toString());
        STATE.getConfigBuilder().include(pravegaProperties);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testListScopesCommand() throws Exception {
        ControllerCommand cmd = new ControllerListScopesCommand(new CommandArgs(Collections.emptyList(), STATE));
        cmd.execute();
        Assert.assertNotNull(cmd.getResponse());
        Assert.assertTrue(cmd.getResponse().contains("_system"));
    }

    @Test
    public void testListStreamsCommand() throws Exception {
        ControllerCommand cmd = new ControllerListStreamsInScopeCommand(new CommandArgs(Collections.singletonList("_system"), STATE));
        cmd.execute();
        Assert.assertNotNull(cmd.getResponse());
    }

    @Test
    public void testListReaderGroupsCommand() throws Exception {
        ControllerCommand cmd = new ControllerListReaderGroupsInScopeCommand(new CommandArgs(Collections.singletonList("_system"), STATE));
        cmd.execute();
        Assert.assertNotNull(cmd.getResponse());
        Assert.assertTrue(cmd.getResponse().contains("commitStreamReaders"));
    }

    @Test
    public void testDescribeScopeCommand() throws Exception {
        ControllerCommand cmd = new ControllerDescribeScopeCommand(new CommandArgs(Collections.singletonList("_system"), STATE));
        cmd.execute();
        Assert.assertNotNull(cmd.getResponse());
        Assert.assertTrue(cmd.getResponse().contains("_system"));
    }
}
