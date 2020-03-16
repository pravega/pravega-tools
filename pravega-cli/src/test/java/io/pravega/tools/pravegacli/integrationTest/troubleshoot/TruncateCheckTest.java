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

import com.google.common.collect.ImmutableSet;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.*;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.troubleshoot.Fault;
import io.pravega.tools.pravegacli.commands.troubleshoot.Record;
import io.pravega.tools.pravegacli.commands.troubleshoot.TruncateCheckCommand;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class TruncateCheckTest {
    private PravegaTablesStoreHelper storeHelper;
    private static final ToolSetupUtils SETUP_UTILS = new ToolSetupUtils();
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();
    private ServiceConfig serviceConfig;
    private CommandArgs commandArgs;
    private volatile StreamMetadataStore store;
    private ScheduledExecutorService executor;
    private TruncateCheckCommand tc;
    private  String tablename;
    private String testStream ;
    private Map<Record, Set<Fault>> faults;

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
    public void initialsetup_store()
    {
        store = SETUP_UTILS.createMetadataStore(executor,serviceConfig,commandArgs);
        SegmentHelper segmentHelper = SETUP_UTILS.getSegmentHelper();
        GrpcAuthHelper authHelper = SETUP_UTILS.getAuthHelper();
        storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
    }

    public void initialsetup_commands()
    {
        commandArgs = new CommandArgs(Arrays.asList(SETUP_UTILS.getScope(), testStream), STATE.get());
        tc= new TruncateCheckCommand(commandArgs);
        serviceConfig = commandArgs.getState().getConfigBuilder().build().getConfig(ServiceConfig::builder);
        executor = commandArgs.getState().getExecutor();

    }
    @Test
    public void executeCommand() throws Exception {
        testStream = "testStream";
        String scope="scope";
        initialsetup_commands();
        initialsetup_store();
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();
        SETUP_UTILS.createTestStream_withconfig(testStream, 1,configuration);
        tablename = SETUP_UTILS.getMetadataTable(testStream,storeHelper).join();
        store.setState(scope, testStream, State.ACTIVE, null, executor).get();

        //checking for unavailability
        VersionedMetadata<StreamTruncationRecord> currentStreamTruncationRecord1 = storeHelper.getEntry(tablename, "truncation", x -> StreamTruncationRecord.fromBytes(x)).get();
        String result1=unavailability_check(currentStreamTruncationRecord1);
        Assert.assertTrue(result1.equalsIgnoreCase("StreamTruncationRecord is corrupted or unavailable"));

        //checking for inconsistency
        VersionedMetadata<StreamTruncationRecord> currentStreamTruncationRecord2 = storeHelper.getEntry(tablename, "truncation", x -> StreamTruncationRecord.fromBytes(x)).get();
        String result2=inconsistency_check(currentStreamTruncationRecord2);
        Assert.assertTrue(result2.equalsIgnoreCase("StreamTruncationRecord inconsistency in regards to updating and segments to delete"));

        //checking for inconsistency between streamcut and segments to delete
        VersionedMetadata<StreamTruncationRecord> currentStreamTruncationRecord3 = storeHelper.getEntry(tablename, "truncation", x -> StreamTruncationRecord.fromBytes(x)).get();
        String result3=segment_count_inconsistency_check(currentStreamTruncationRecord3);
        Assert.assertTrue(result3.equalsIgnoreCase("Fault in the StreamTruncationRecord in regards to segments deletion, segments ahead of stream cut being deleted"));

    }

    private String unavailability_check(VersionedMetadata<StreamTruncationRecord> currentStreamTruncationRecordMetadata)
    {
        Version version=currentStreamTruncationRecordMetadata.getVersion();
        storeHelper.removeEntry(tablename,"truncation",version).join();
        faults=tc.check(store,executor);
        storeHelper.addNewEntry(tablename, "truncation", currentStreamTruncationRecordMetadata.getObject().toBytes()).join();
        return (SETUP_UTILS.faultvalue(faults));

    }
    private String inconsistency_check(VersionedMetadata<StreamTruncationRecord> currentStreamTruncationRecordMetadata)
    {
        StreamTruncationRecord oldRecord=currentStreamTruncationRecordMetadata.getObject();
        ImmutableSet<Long> toDelete=ImmutableSet.of(4L,5L);
        StreamTruncationRecord newRecord=new StreamTruncationRecord(oldRecord.getStreamCut(),oldRecord.getSpan(),oldRecord.getDeletedSegments(),toDelete,oldRecord.getSizeTill(),false);
        Version v=currentStreamTruncationRecordMetadata.getVersion();
        storeHelper.removeEntry(tablename, "truncation",v).join();
        storeHelper.addNewEntry(tablename,"truncation",newRecord.toBytes()).join();
        StreamMetadataStore mystore = SETUP_UTILS.createMetadataStore(executor,serviceConfig,commandArgs);
        faults = tc.check(mystore, executor);
        VersionedMetadata<StreamTruncationRecord> newStreamTruncationRecord1 = storeHelper.getEntry(tablename, "truncation", x -> StreamTruncationRecord.fromBytes(x)).join();
        changingBackToOrginalState(newStreamTruncationRecord1,oldRecord);
        return(SETUP_UTILS.faultvalue(faults));
    }

    private String segment_count_inconsistency_check(VersionedMetadata<StreamTruncationRecord> currentStreamTruncationRecordMetadata)
    {
        StreamTruncationRecord oldRecord=currentStreamTruncationRecordMetadata.getObject();
        ImmutableSet<Long> toDelete=ImmutableSet.of(4L,5L);
        StreamTruncationRecord newRecord=new StreamTruncationRecord(oldRecord.getStreamCut(),oldRecord.getSpan(),oldRecord.getDeletedSegments(),toDelete,oldRecord.getSizeTill(),true);
        Version v=currentStreamTruncationRecordMetadata.getVersion();
        storeHelper.removeEntry(tablename, "truncation",v).join();
        storeHelper.addNewEntry(tablename,"truncation",newRecord.toBytes()).join();
        StreamMetadataStore mystore = SETUP_UTILS.createMetadataStore(executor,serviceConfig,commandArgs);
        faults = tc.check(mystore, executor);
        VersionedMetadata<StreamTruncationRecord> newStreamTruncationRecord1 = storeHelper.getEntry(tablename, "truncation", x -> StreamTruncationRecord.fromBytes(x)).join();
        changingBackToOrginalState(newStreamTruncationRecord1,oldRecord);
        return(SETUP_UTILS.faultvalue(faults));
    }
    private void changingBackToOrginalState(VersionedMetadata<StreamTruncationRecord> currentStreamTruncationRecordMetadata, StreamTruncationRecord oldStreamTruncationRecord )
    {
        Version version = currentStreamTruncationRecordMetadata.getVersion();
        storeHelper.removeEntry(tablename, "truncation", version).join();
        storeHelper.addNewEntry(tablename, "truncation", oldStreamTruncationRecord.toBytes()).join();
    }

}
