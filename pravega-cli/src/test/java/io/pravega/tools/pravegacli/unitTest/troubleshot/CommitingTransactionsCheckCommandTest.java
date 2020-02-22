/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli.unitTest.troubleshot;

import com.google.common.collect.ImmutableList;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.Version;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
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
import org.mockito.Mockito;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class CommitingTransactionsCheckCommandTest {
    // Setup utility.
    private Map<Record, Set<Fault>> faults;
    private static final ToolSetupUtils SETUP_UTILS = new ToolSetupUtils();
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();
    private ServiceConfig serviceConfig;
    private CommandArgs commandArgs;
    private volatile StreamMetadataStore store;
    private ScheduledExecutorService executor;
    private CommittingTransactionsCheckCommand ct;
    private String testStream ;
    private StreamMetadataStore mockstore;

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
    }

    @Test
    public void executeCommand() throws Exception {
        testStream="testStream";
        initialsetup_commands();
        initialsetup_store();
        SETUP_UTILS.createTestStream(testStream, 1);
        mockstore= Mockito.mock(StreamMetadataStore.class);

        //checking for inconsistency
        String result= inconsistency_check();
        Assert.assertTrue(result.contains("The corresponding EpochRecord is corrupted or does not exist."));
    }

    public String inconsistency_check() {
        UUID v1 = new UUID(0, 1);
        UUID v2 = new UUID(1, 2);
        ImmutableList<UUID> list = ImmutableList.of(v1, v2);
        CommittingTransactionsRecord newTransactionRecord = new CommittingTransactionsRecord(0, list, 0);
        Version.IntVersion v = Version.IntVersion.builder().intValue(0).build();
        VersionedMetadata<CommittingTransactionsRecord> mockVersionRecord=new VersionedMetadata<>(newTransactionRecord,v);
        Mockito.when(mockstore.getVersionedCommittingTransactionsRecord("scope", testStream, null, executor)).
                thenReturn(CompletableFuture.completedFuture(mockVersionRecord));

        Mockito.when( mockstore.getEpoch("scope", testStream, newTransactionRecord.getNewTxnEpoch(),
              null, executor)).thenReturn( store.getEpoch("scope", testStream, newTransactionRecord.getNewTxnEpoch(),
              null, executor));

        int chunkNumber=newTransactionRecord.getNewTxnEpoch()/ HistoryTimeSeries.HISTORY_CHUNK_SIZE;
         Mockito.when( mockstore.getHistoryTimeSeriesChunk("scope", testStream,
                chunkNumber, null, executor)).
                thenReturn(store.getHistoryTimeSeriesChunk("scope", testStream,
                        chunkNumber, null, executor));
         //calling the check
        faults = ct.check(mockstore, executor);
        //returning to orignal value
        return SETUP_UTILS.faultvalue(faults);

    }
}
