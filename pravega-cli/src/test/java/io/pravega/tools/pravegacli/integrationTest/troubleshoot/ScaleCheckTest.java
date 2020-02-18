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
import io.pravega.common.Exceptions;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.*;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.troubleshoot.Fault;
import io.pravega.tools.pravegacli.commands.troubleshoot.Record;
import io.pravega.tools.pravegacli.commands.troubleshoot.ScaleCheckCommand;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class ScaleCheckTest {
    private SegmentHelper segmentHelper;
    private GrpcAuthHelper authHelper;
    private PravegaTablesStoreHelper storeHelper;
    private static final ToolSetupUtils SETUP_UTILS = new ToolSetupUtils();
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();
    private ServiceConfig serviceConfig;
    private CommandArgs commandArgs;
    private AtomicReference<String> idRef=new AtomicReference<>(null);;
    private volatile StreamMetadataStore store;
    private ScheduledExecutorService executor;
    private ScaleCheckCommand sc;
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
        segmentHelper=SETUP_UTILS.getSegmentHelper();
        authHelper=SETUP_UTILS.getAuthHelper();
        storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
    }

    public void initialsetup_commands()
    {
        commandArgs = new CommandArgs(Arrays.asList(SETUP_UTILS.getScope(), testStream), STATE.get());
        sc= new ScaleCheckCommand(commandArgs);
        serviceConfig = commandArgs.getState().getConfigBuilder().build().getConfig(ServiceConfig::builder);
        executor = commandArgs.getState().getExecutor();

    }
    @Test
    public void executeCommand() throws Exception {
        final String scope = "scope";
        final String stream = "testStream";
        testStream="testStream";
        initialsetup_commands();
        initialsetup_store();
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();
        long start = System.currentTimeMillis();
        store.createStream(scope, stream, configuration, start, null, executor).get();
        store.setState(scope, stream, State.ACTIVE, null, executor).get();
        tablename = SETUP_UTILS.getMetadataTable(testStream,storeHelper).join();
        VersionedMetadata<EpochTransitionRecord> currentEpochTransitionRecordMetadata= do_scale();

        //checking for unavalability
        VersionedMetadata<EpochTransitionRecord> currentEpochTransitionRecordMetadata1 = storeHelper.getEntry(tablename, "epochTransition", x -> EpochTransitionRecord.fromBytes(x)).get();
        Version version = currentEpochTransitionRecordMetadata1.getVersion();
        storeHelper.removeEntry(tablename, "epochTransition", version).join();
        storeHelper.addNewEntry(tablename, "epochTransition", currentEpochTransitionRecordMetadata.getObject().toBytes()).join();
        currentEpochTransitionRecordMetadata1 = storeHelper.getEntry(tablename, "epochTransition", x -> EpochTransitionRecord.fromBytes(x)).get();
        String result1 =unvalabilityCheck(currentEpochTransitionRecordMetadata1);
        Assert.assertEquals(result1,"EpochTransitionRecord is corrupted or unavailable");

        //checking for inconsistency
        VersionedMetadata<EpochTransitionRecord> currentEpochTransitionRecordMetadata2= storeHelper.getEntry(tablename, "epochTransition", x -> EpochTransitionRecord.fromBytes(x)).get();
        String result2=Inconsistency_check(currentEpochTransitionRecordMetadata2);
        System.out.println("value of result2 = "+result2);
        Assert.assertEquals(result2,"HistoryTimeSeriesRecord and EpochTransitionRecord mismatch in the sealed segments");
    }

    public VersionedMetadata<EpochTransitionRecord> do_scale()
    {
        String scope="scope";
        String stream="testStream";
        // set minimum number of segments to 1 so that we can also test scale downs
        // region idempotent

        long scaleTs = System.currentTimeMillis();
        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        List<Long> scale1SealedSegments = Collections.singletonList(1L);

        // 1. submit scale
        VersionedMetadata<EpochTransitionRecord> empty = store.getEpochTransition(scope, stream, null, executor).join();
        VersionedMetadata<EpochTransitionRecord> response = store.submitScale(scope, stream, scale1SealedSegments,
                Arrays.asList(segment1, segment2), scaleTs, null, null, executor).join();
        Map<Long, Map.Entry<Double, Double>> scale1SegmentsCreated = response.getObject().getNewSegmentsWithRange();
        final int scale1ActiveEpoch = response.getObject().getActiveEpoch();
        assertEquals(0, scale1ActiveEpoch);

        // rerun start scale with old epoch transition. should throw write conflict
        AssertExtensions.assertSuppliedFutureThrows("", () -> store.submitScale(scope, stream, scale1SealedSegments,
                Arrays.asList(segment1, segment2), scaleTs, empty, null, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);

        // rerun start scale with null epoch transition, should be idempotent
        response = store.submitScale(scope, stream, scale1SealedSegments,
                Arrays.asList(segment1, segment2), scaleTs, null, null, executor).join();
        assertEquals(response.getObject().getNewSegmentsWithRange(), scale1SegmentsCreated);

        VersionedMetadata<State> state = store.getVersionedState(scope, stream, null, executor).join();
        state = store.updateVersionedState(scope, stream, State.SCALING, state, null, executor).join();
        response = store.startScale(scope, stream, false, response, state, null, executor).join();

        // 2. scale new segments created
        store.scaleCreateNewEpochs(scope, stream, response, null, executor).join();

        // rerun start scale and new segments created
        response = store.submitScale(scope, stream, scale1SealedSegments,
                Arrays.asList(segment1, segment2), scaleTs, null, null, executor).join();
        assertEquals(response.getObject().getNewSegmentsWithRange(), scale1SegmentsCreated);

        response = store.startScale(scope, stream, false, response, state, null, executor).join();
        store.scaleCreateNewEpochs(scope, stream, response, null, executor).join();

        // 3. scale segments sealed -- this will complete scale
        store.scaleSegmentsSealed(scope, stream, scale1SealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), response,
                null, executor).join();
        VersionedMetadata<EpochTransitionRecord> currentEpochTransitionRecordMetadata= storeHelper.getEntry(tablename, "epochTransition", x -> EpochTransitionRecord.fromBytes(x)).join();
        store.completeScale(scope, stream, response, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).join();
        return currentEpochTransitionRecordMetadata;
    }


    public void changingBackToOrginalState(VersionedMetadata<EpochTransitionRecord> currentEpochVersionMetadata, EpochTransitionRecord oldEpochTransitionRecord )
    {
        Version version = currentEpochVersionMetadata.getVersion();
        EpochTransitionRecord currentEpochTransitionRecord=currentEpochVersionMetadata.getObject();
        storeHelper.removeEntry(tablename, "epochTransition", version).join();
        storeHelper.addNewEntry(tablename, "epochTransition", oldEpochTransitionRecord.toBytes()).join();

    }

    public String unvalabilityCheck( VersionedMetadata<EpochTransitionRecord> currentEpochTransitionRecordMetadata )
    {

        EpochTransitionRecord currentEpochTransitionRecord=currentEpochTransitionRecordMetadata.getObject();
        Version version=currentEpochTransitionRecordMetadata.getVersion();
        storeHelper.removeEntry(tablename, "epochTransition", version).join();
        faults=sc.check(store,executor);
        storeHelper.addNewEntry(tablename, "epochTransition",currentEpochTransitionRecord.toBytes()).join();
        return (SETUP_UTILS.faultvalue(faults));
    }

  public String Inconsistency_check(VersionedMetadata<EpochTransitionRecord> currentEpochTransitionRecordMetadata)
  {
      Version version=currentEpochTransitionRecordMetadata.getVersion();
      EpochTransitionRecord currentEpochTransitionRecord=currentEpochTransitionRecordMetadata.getObject();
      HashSet<Long> segmentIds=new HashSet<>();
      segmentIds.add(1L);
      segmentIds.add(2L);
      ImmutableSet<Long> immutableSegmentid = ImmutableSet.copyOf(segmentIds);
      EpochTransitionRecord newEpochTransitionRecord=new EpochTransitionRecord(currentEpochTransitionRecord.getActiveEpoch(), currentEpochTransitionRecord.getTime(),immutableSegmentid,currentEpochTransitionRecord.getNewSegmentsWithRange());
      storeHelper.removeEntry(tablename, "epochTransition", version).join();
      storeHelper.addNewEntry(tablename,"epochTransition",newEpochTransitionRecord.toBytes()).join();
      StreamMetadataStore mystore = SETUP_UTILS.createMetadataStore(executor,serviceConfig,commandArgs);
      faults=sc.check(mystore,executor);
      VersionedMetadata<EpochTransitionRecord> newEpochTransitionRecordMetadata = storeHelper.getEntry(tablename, "epochTransition", x -> EpochTransitionRecord.fromBytes(x)).join();
      changingBackToOrginalState(newEpochTransitionRecordMetadata,currentEpochTransitionRecord);
      return (SETUP_UTILS.faultvalue(faults));
  }

}
