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

import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.Version;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.troubleshoot.UpdateCheckCommand;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class UpdateCommandTest {
    // Setup utility.
    private static final ToolSetupUtils SETUP_UTILS = new ToolSetupUtils();
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();
    private ServiceConfig serviceConfig;
    private CommandArgs commandArgs;
    private volatile StreamMetadataStore store;
    private ScheduledExecutorService executor;
    private UpdateCheckCommand updatecheck;
    private  String tablename;
    private String testStream;

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

    public void initialStoreSetup() {


        store = SETUP_UTILS.createMetadataStore(executor, serviceConfig, commandArgs);
    }
    public void initialSetupCommands() {
        commandArgs = new CommandArgs(Arrays.asList(SETUP_UTILS.getScope(), testStream), STATE.get());
        updatecheck = new UpdateCheckCommand(commandArgs);
        serviceConfig = commandArgs.getState().getConfigBuilder().build().getConfig(ServiceConfig::builder);
        executor = commandArgs.getState().getExecutor();
    }

    @Test
    public void executeCommand() throws Exception {
        testStream = "testStream";
        SETUP_UTILS.createTestStream(testStream, 1);
        initialSetupCommands();
        initialStoreSetup();

        //mocking the store
        StreamMetadataStore mystoremock = Mockito.mock(StreamMetadataStore.class);
        //checking for fault if configurationRecord is null
        String result = SETUP_UTILS.faultvalue(updatecheck.check(mystoremock, executor));
        Assert.assertTrue("StreamConfigurationRecord consistency check requires human intervention".equalsIgnoreCase(result));

        //checking for correct case
        StreamConfigurationRecord presentStreamConfigurationRecord = store.getConfigurationRecord("scope", testStream, null, executor).join().getObject();
        StreamConfigurationRecord mockStreamConfigurationRecord = new StreamConfigurationRecord(presentStreamConfigurationRecord.getScope(), presentStreamConfigurationRecord.getStreamName(), presentStreamConfigurationRecord.getStreamConfiguration(), true);
        Version.IntVersion ver = Version.IntVersion.builder().intValue(0).build();
        VersionedMetadata<StreamConfigurationRecord> mockVersionRecord = new VersionedMetadata<>(mockStreamConfigurationRecord, ver);
        Mockito.when(mystoremock.getConfigurationRecord("scope", testStream, null, executor)).thenReturn(CompletableFuture.completedFuture(mockVersionRecord));
        String result2 = SETUP_UTILS.faultvalue(updatecheck.check(mystoremock, executor));
        Assert.assertTrue("".equalsIgnoreCase(result2));
    }
}
