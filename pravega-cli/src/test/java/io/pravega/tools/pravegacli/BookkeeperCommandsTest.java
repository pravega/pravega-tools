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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test basic functionality of Bookkeeper commands.
 */
public class BookkeeperCommandsTest extends BookKeeperClusterTestCase {

    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    public BookkeeperCommandsTest() {
        super(3);
    }

    @Before
    public void setUp() throws Exception {
        baseConf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.FlatLedgerManagerFactory");
        baseClientConf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.FlatLedgerManagerFactory");
        super.setUp();

        STATE.set(new AdminCommandState());
        Properties bkProperties = new Properties();
        bkProperties.setProperty("pravegaservice.containerCount", "4");
        bkProperties.setProperty("pravegaservice.zkURL", zkUtil.getZooKeeperConnectString());
        bkProperties.setProperty("bookkeeper.bkLedgerPath", "/ledgers");
        STATE.get().getConfigBuilder().include(bkProperties);
    }


    @Test
    public void testBookKeeperListCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("bk list", STATE.get());
        Assert.assertTrue(commandResult.contains("Log 0"));
    }

    @Test
    public void testBookKeeperDetailsCommand() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setMetadataServiceUri(zkUtil.getZooKeeperConnectString()).setZkTimeout(30000);
        BookKeeper bookkeeper = new BookKeeper(conf);
        LedgerHandle lh = bookkeeper.createLedger(3, 3, 2, BookKeeper.DigestType.MAC, "hello".getBytes());
        String commandResult = TestUtils.executeCommand("bk details 0", STATE.get());
        Assert.assertTrue(commandResult.contains("Ledger"));
        lh.close();
    }
}