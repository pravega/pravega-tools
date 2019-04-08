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

import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import lombok.Cleanup;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BookkeeperCommandsTest extends BookKeeperClusterTestCase {

    private static AdminCommandState STATE;

    public BookkeeperCommandsTest() {
        super(5);
    }

    @Before
    public void setUp() throws Exception {
        baseConf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.FlatLedgerManagerFactory");
        baseClientConf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.FlatLedgerManagerFactory");
        super.setUp();

        STATE = new AdminCommandState();
        Properties bkProperties = new Properties();
        bkProperties.setProperty("pravegaservice.containerCount", "4");
        bkProperties.setProperty("pravegaservice.zkURL", zkUtil.getZooKeeperConnectString());
        bkProperties.setProperty("bookkeeper.bkLedgerPath", "/ledgers");
        STATE.getConfigBuilder().include(bkProperties);
    }


    @Test
    public void testBookKeeperListCommand() throws Exception {
        Parser.Command pc = Parser.parse("bk list");
        CommandArgs args = new CommandArgs(pc.getArgs(), STATE);
        Command cmd = Command.Factory.get(pc.getComponent(), pc.getName(), args);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true, "UTF-8")) {
            cmd.setOut(ps);
            cmd.execute();
        }

        String data = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        Assert.assertTrue(data.contains("Log 0"));
    }
}