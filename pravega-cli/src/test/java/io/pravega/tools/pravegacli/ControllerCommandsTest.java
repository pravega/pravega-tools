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
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.controller.ControllerListScopesCommand;
import io.pravega.tools.pravegacli.commands.utils.ConfigUtils;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ControllerCommandsTest {
    // Setup utility.
    private static final SetupUtils SETUP_UTILS = new SetupUtils();
    private static AdminCommandState STATE;

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.startAllServices();
        STATE = new AdminCommandState();
        ConfigUtils.loadPropertiesFromFile(STATE);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testListScopesCommand() throws Exception {
        Command cmd = new ControllerListScopesCommand(new CommandArgs(Collections.emptyList(), STATE));
        cmd.execute();
    }
}
