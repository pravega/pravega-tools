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
import java.util.Properties;
import lombok.Cleanup;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;

public class BookkeeperCommandsTest extends BookKeeperClusterTestCase {

    public BookkeeperCommandsTest() {
        super(5);
    }

    @Before
    public void setUp() throws Exception {
        baseConf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.FlatLedgerManagerFactory");
        baseClientConf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.FlatLedgerManagerFactory");
        super.setUp();
    }

    @Override
    protected String getMetadataServiceUri(String ledgersRootPath) {
        return zkUtil.getMetadataServiceUri(ledgersRootPath, "flat");
    }

    @Test
    public void testBookKeeperListCommand() throws Exception {
        Properties newValues = new Properties();
        newValues.setProperty("pravegaservice.containerCount", "4");
        newValues.setProperty("pravegaservice.zkURL", "localhost:2181");
        newValues.setProperty("bookkeeper.bkLedgerPath", "/pravega/bookkeeper/ledgers");
        newValues.setProperty("bookkeeper.zkMetadataPath", "/segmentstore/containers");
        @Cleanup
        AdminCommandState state = new AdminCommandState();
        Parser.Command pc = Parser.parse("bk list");
        CommandArgs args = new CommandArgs(pc.getArgs(), state);
        args.getState().getConfigBuilder().include(newValues);
        Command cmd = Command.Factory.get(pc.getComponent(), pc.getName(), args);
        cmd.execute();
    }
}
