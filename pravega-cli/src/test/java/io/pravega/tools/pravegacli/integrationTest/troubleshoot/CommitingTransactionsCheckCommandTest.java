/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli.integrationTest.troubleshoot;

import com.google.common.collect.ImmutableList;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.PravegaTablesStoreHelper;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.Version;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.troubleshoot.CommittingTransactionsCheckCommand;
import io.pravega.tools.pravegacli.commands.troubleshoot.Fault;
import io.pravega.tools.pravegacli.commands.troubleshoot.Record;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;


public class CommitingTransactionsCheckCommandTest {
    // Setup utility.
    private Map<Record, Set<Fault>> faults;
    SegmentHelper segmentHelper;
    private GrpcAuthHelper authHelper;
    private PravegaTablesStoreHelper storeHelper;
    private static final ToolSetupUtils SETUP_UTILS = new ToolSetupUtils();
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();
    private ServiceConfig serviceConfig;
    private CommandArgs commandArgs;
    private volatile StreamMetadataStore store;
    private ScheduledExecutorService executor;
    private CommittingTransactionsCheckCommand ct;
    private  String tablename;
    private String testStream ;

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.startAllServices();
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controllerRestUri", SETUP_UTILS.getControllerRestUri().toString());
        pravegaProperties.setProperty("pravegaservice.zkURL", SETUP_UTILS.getZkTestServer().getConnectString());
        pravegaProperties.setProperty("pravegaservice.containerCount", "4");
        STATE.get().getConfigBuilder().include(pravegaProperties);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
    }


    public void initialsetup_commands()
    {
        commandArgs = new CommandArgs(Arrays.asList(SETUP_UTILS.getScope(), testStream), STATE.get());
        ct = new CommittingTransactionsCheckCommand(commandArgs);
        serviceConfig = commandArgs.getState().getConfigBuilder().build().getConfig(ServiceConfig::builder);
        executor = commandArgs.getState().getExecutor();

    }
    public void initialsetup_store()
    {
        store = SETUP_UTILS.createMetadataStore(executor,serviceConfig,commandArgs);
        segmentHelper=SETUP_UTILS.getSegmentHelper();
        authHelper=SETUP_UTILS.getAuthHelper();
        storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
    }

    @Test
    public void executeCommand() throws Exception {
        testStream="testStream";
        initialsetup_commands();
        initialsetup_store();
        SETUP_UTILS.createTestStream(testStream, 1);
        tablename=SETUP_UTILS.getMetadataTable(testStream,storeHelper).join();

        //checking for unavalability
        VersionedMetadata<CommittingTransactionsRecord> committingVersionMetadata1=storeHelper.getEntry(tablename,"committingTxns", x -> CommittingTransactionsRecord.fromBytes(x)).join();
        String result1 = unavaliblity_check(committingVersionMetadata1);
        Assert.assertEquals(result1,"CommittingTransactionsRecord is corrupted or unavailable");

        //checking for inconsistency
        VersionedMetadata<CommittingTransactionsRecord> committingVersionMetadata2=storeHelper.getEntry(tablename,"committingTxns", x -> CommittingTransactionsRecord.fromBytes(x)).join();
        String result2= inconsistency_check(committingVersionMetadata2);
        Assert.assertEquals(result2,"Duplicate txn epoch: 1 is corrupted or does not exist.");

    }

    public String unavaliblity_check(VersionedMetadata<CommittingTransactionsRecord> committingVersionMetadata)
    {
        Version ver=committingVersionMetadata.getVersion() ;
        storeHelper.removeEntry(tablename, "committingTxns",ver).join();
        faults=ct.check(store,executor);
        String result=SETUP_UTILS.faultvalue(faults);
        storeHelper.addNewEntry(tablename,"committingTxns",committingVersionMetadata.getObject().toBytes()).join();
        return result;
    }
    public String inconsistency_check(VersionedMetadata<CommittingTransactionsRecord> committingVersionMetadata) {

        Version version = committingVersionMetadata.getVersion();
        storeHelper.removeEntry(tablename, "committingTxns", version).join();
        UUID v1 = new UUID(0, 1);
        UUID v2 = new UUID(1, 2);
        ImmutableList<UUID> list = ImmutableList.of(v1, v2);
        CommittingTransactionsRecord newTransactionRecord = new CommittingTransactionsRecord(0, list, 0);
        storeHelper.addNewEntry(tablename, "committingTxns", newTransactionRecord.toBytes()).join();
        faults = ct.check(store, executor);

        //returning to orignal value
        VersionedMetadata<CommittingTransactionsRecord> committingVersionMetadata1=storeHelper.getEntry(tablename,"committingTxns", x -> CommittingTransactionsRecord.fromBytes(x)).join();
        storeHelper.removeEntry(tablename,"committingTxns",committingVersionMetadata1.getVersion()).join();
        storeHelper.addNewEntry(tablename,"committingTxns", committingVersionMetadata.getObject().toBytes()).join();
        return SETUP_UTILS.faultvalue(faults);
    }
}
