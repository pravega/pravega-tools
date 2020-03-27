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
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.PravegaTablesStoreHelper;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.Version;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.troubleshoot.Fault;
import io.pravega.tools.pravegacli.commands.troubleshoot.GeneralCheckCommand;
import io.pravega.tools.pravegacli.commands.troubleshoot.Record;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class GeneralCheckTest {

    // Setup utility.
    private Map<Record, Set<Fault>> faults;
    SegmentHelper segmentHelper;
    private PravegaTablesStoreHelper storeHelper;
    private static final ToolSetupUtils SETUP_UTILS = new ToolSetupUtils();
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();
    private ServiceConfig serviceConfig;
    private CommandArgs commandArgs;
    private volatile StreamMetadataStore store;
    private ScheduledExecutorService executor;
    private GeneralCheckCommand genralCheck;
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

    public void initialSetupCommands() {
        commandArgs = new CommandArgs(Arrays.asList(SETUP_UTILS.getScope(), testStream), STATE.get());
        genralCheck = new GeneralCheckCommand(commandArgs);
        serviceConfig = commandArgs.getState().getConfigBuilder().build().getConfig(ServiceConfig::builder);
        executor = commandArgs.getState().getExecutor();
    }

    public void initialStoreSetup() {
       store = SETUP_UTILS.createMetadataStore(executor, serviceConfig, commandArgs);
        segmentHelper = SETUP_UTILS.getSegmentHelper();
        GrpcAuthHelper authHelper = SETUP_UTILS.getAuthHelper();
        storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
    }

    @Test
    public void executeCommand() throws Exception {
        testStream = "testStream";
        initialSetupCommands();
        initialStoreSetup();
        SETUP_UTILS.createTestStream(testStream, 1);
        Map<String, StreamConfiguration> streamInScope = store.listStreamsInScope("scope").get();
        tablename = SETUP_UTILS.getMetadataTable(testStream, storeHelper).join();

        //testing for reference missmatch
        VersionedMetadata<EpochRecord> currentEpochVersionMetadata = storeHelper.getEntry(tablename, "epochRecord-0", x -> EpochRecord.fromBytes(x)).get();
        String result = checkingReferenceEpochMissmatch(currentEpochVersionMetadata);
        Assert.assertTrue(result.equalsIgnoreCase("Reference epoch mismatch."));

        //testing for epoch missmatch
        initialStoreSetup();
        VersionedMetadata<EpochRecord> currentEpochVersionMetadata2 = storeHelper.getEntry(tablename, "epochRecord-0", x -> EpochRecord.fromBytes(x)).join();
        System.err.println(currentEpochVersionMetadata2.getObject() + " " + currentEpochVersionMetadata2.getVersion());
        String result2 = checkingEpochMissmatch(currentEpochVersionMetadata2);
        Assert.assertTrue(result2.equalsIgnoreCase("Epoch mismatch : May or may not be the correct record."));

        //testing for creation_time missmatch
        initialStoreSetup();
        VersionedMetadata<EpochRecord> currentEpochVersionMetadata3 = storeHelper.getEntry(tablename, "epochRecord-0", x -> EpochRecord.fromBytes(x)).join();
        String result3 = checkingCreationTimeMissmatch(currentEpochVersionMetadata3);
        Assert.assertTrue(result3.equalsIgnoreCase("Creation time mismatch."));

        //testing for Segments missmatch
        initialStoreSetup();
        VersionedMetadata<EpochRecord> currentEpochVersionMetadata4 = storeHelper.getEntry(tablename, "epochRecord-0", x -> EpochRecord.fromBytes(x)).get();
        String result4 = checkingSegmentsMissmatch(currentEpochVersionMetadata4);
        Assert.assertTrue(result4.equalsIgnoreCase("Segment data mismatch."));
    }

    public String checkingReferenceEpochMissmatch(VersionedMetadata<EpochRecord> currentEpochVersionMetadata) {
        Version version = currentEpochVersionMetadata.getVersion();
        EpochRecord currentEpoch = currentEpochVersionMetadata.getObject();
        EpochRecord newEpoch = new EpochRecord(currentEpoch.getEpoch(), 4, currentEpoch.getSegments(), currentEpoch.getCreationTime());
        storeHelper.updateEntry(tablename, "epochRecord-0", newEpoch.toBytes(), version).join();
        StreamMetadataStore mystore = SETUP_UTILS.createMetadataStore(executor, serviceConfig, commandArgs);
        faults = genralCheck.check(mystore, executor);
        VersionedMetadata<EpochRecord> EpochVersionMetadata = storeHelper.getEntry(tablename, "epochRecord-0", x -> EpochRecord.fromBytes(x)).join();
        changingBackToOrginalState(EpochVersionMetadata, currentEpoch);
        return (SETUP_UTILS.faultvalue(faults));
    }

    public String checkingEpochMissmatch(VersionedMetadata<EpochRecord> currentEpochVersionMetadata) {
        Version version = currentEpochVersionMetadata.getVersion();
        EpochRecord currentEpoch = currentEpochVersionMetadata.getObject();
        EpochRecord newEpoch = new EpochRecord(2, currentEpoch.getReferenceEpoch(), currentEpoch.getSegments(), currentEpoch.getCreationTime());
        storeHelper.updateEntry(tablename, "epochRecord-0", newEpoch.toBytes(), version).join();
        VersionedMetadata<EpochRecord> currentEpochVersionMetadata2 = storeHelper.getEntry(tablename, "epochRecord-0", x -> EpochRecord.fromBytes(x)).join();
        StreamMetadataStore mystore = SETUP_UTILS.createMetadataStore(executor, serviceConfig, commandArgs);
        faults = genralCheck.check(mystore, executor);
        VersionedMetadata<EpochRecord> EpochVersionMetadata = storeHelper.getEntry(tablename, "epochRecord-0", x -> EpochRecord.fromBytes(x)).join();
        changingBackToOrginalState(EpochVersionMetadata, currentEpoch);
        return (SETUP_UTILS.faultvalue(faults));
    }

    public String checkingCreationTimeMissmatch(VersionedMetadata<EpochRecord> currentEpochVersionMetadata) {
        Version version = currentEpochVersionMetadata.getVersion();
        EpochRecord currentEpoch = currentEpochVersionMetadata.getObject();
        EpochRecord newEpoch = new EpochRecord(currentEpoch.getEpoch(), currentEpoch.getReferenceEpoch(), currentEpoch.getSegments(), 2);
        storeHelper.updateEntry(tablename, "epochRecord-0", newEpoch.toBytes(), version).join();
        StreamMetadataStore mystore = SETUP_UTILS.createMetadataStore(executor, serviceConfig, commandArgs);
        faults = genralCheck.check(mystore, executor);
        VersionedMetadata<EpochRecord> EpochVersionMetadata = storeHelper.getEntry(tablename, "epochRecord-0", x -> EpochRecord.fromBytes(x)).join();
        changingBackToOrginalState(EpochVersionMetadata, currentEpoch);
        return (SETUP_UTILS.faultvalue(faults));
    }

    public String checkingSegmentsMissmatch(VersionedMetadata<EpochRecord> currentEpochVersionMetadata) {
        Version version = currentEpochVersionMetadata.getVersion();
        EpochRecord currentEpoch = currentEpochVersionMetadata.getObject();
        List<StreamSegmentRecord> currentSegmentsList = currentEpoch.getSegments();
        StreamSegmentRecord currentStreamSegmentRecord = currentSegmentsList.get(0);
        long creationTime = store.getEpoch("scope", testStream, 0, null, executor).join().getCreationTime();
        StreamSegmentRecord newStreamSegmentRecord = new StreamSegmentRecord(4, currentStreamSegmentRecord.getCreationEpoch(), creationTime, currentStreamSegmentRecord.getKeyStart(), currentStreamSegmentRecord.getKeyEnd());
        List<StreamSegmentRecord> newSegmentsList = new ArrayList<>();
        newSegmentsList.add(newStreamSegmentRecord);
        ImmutableList<StreamSegmentRecord> immutableList = ImmutableList.copyOf(newSegmentsList);
        EpochRecord newEpoch = new EpochRecord(currentEpoch.getEpoch(), currentEpoch.getReferenceEpoch(), immutableList, creationTime);
        storeHelper.removeEntry(tablename, "epochRecord-0", version).join();
        storeHelper.addNewEntry(tablename, "epochRecord-0", newEpoch.toBytes()).join();
        StreamMetadataStore mystore = SETUP_UTILS.createMetadataStore(executor, serviceConfig, commandArgs);
        faults = genralCheck.check(mystore, executor);
        VersionedMetadata<EpochRecord> EpochVersionMetadata = storeHelper.getEntry(tablename, "epochRecord-0", x -> EpochRecord.fromBytes(x)).join();
        changingBackToOrginalState(EpochVersionMetadata, currentEpoch);
        return (SETUP_UTILS.faultvalue(faults));
    }

    public void changingBackToOrginalState(VersionedMetadata<EpochRecord> currentEpochVersionMetadata, EpochRecord oldEpoch) {
        Version version = currentEpochVersionMetadata.getVersion();
        storeHelper.removeEntry(tablename, "epochRecord-0", version).join();
        storeHelper.addNewEntry(tablename, "epochRecord-0", oldEpoch.toBytes()).join();
    }

}