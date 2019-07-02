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

import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.*;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.test.integration.utils.SetupUtils;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.troubleshoot.CommittingTransactionsCheckCommand;
import io.pravega.tools.pravegacli.commands.troubleshoot.Fault;
import io.pravega.tools.pravegacli.commands.troubleshoot.Record;
import io.pravega.tools.pravegacli.commands.troubleshoot.ScaleCheckCommand;
import io.pravega.tools.pravegacli.commands.troubleshoot.TruncateCheckCommand;
import io.pravega.tools.pravegacli.commands.troubleshoot.UpdateCheckCommand;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputFaults;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Validate basic troubleshoot commands
 */
public class TroubleshootCommandsTest {
    // Setup utility.
    private static final SetupUtils SETUP_UTILS = new SetupUtils();
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    @Rule
    public final Timeout globalTimeout = new Timeout(360, TimeUnit.SECONDS);

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.startAllServices();
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controllerRestUri", SETUP_UTILS.getControllerRestUri().toString());
        pravegaProperties.setProperty("pravegaservice.zkURL", "localhost:2181");
        pravegaProperties.setProperty("pravegaservice.containerCount", "4");
        STATE.get().getConfigBuilder().include(pravegaProperties);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testGeneralCheck() throws Exception {

    }

    @Test
    public void testUpdateCheck() throws Exception {
        List<String> args = new ArrayList<>();
        args.add("test-scope");
        args.add("test-stream");
        Version v = mock(Version.class);

        UpdateCheckCommand update = new UpdateCheckCommand(new CommandArgs(args, STATE.get()));

        ExtendedStreamMetadataStore spyStore = spy(ExtendedStreamMetadataStore.class);
        Map<Record, Set<Fault>> faults;

        // Test 1: StreamConfigurationRecord is corrupted or unavailable.
        doReturn(Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "no configuration record found")))
                .when(spyStore).getConfigurationRecord(args.get(0), args.get(1), null, null);

        faults = update.check(spyStore, null);

        Assert.assertTrue("Test for if unavailable",
                outputFaults(faults).contains("StreamConfigurationRecord is corrupted or unavailable"));

        // Test 2: StreamConfigurationRecord exists.
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(2))
                .retentionPolicy(RetentionPolicy.bySizeBytes(1000)).build();

        StreamConfigurationRecord record = StreamConfigurationRecord.builder()
                .scope(args.get(0))
                .streamName(args.get(1))
                .updating(false)
                .streamConfiguration(config).build();

        doReturn(CompletableFuture.completedFuture(new VersionedMetadata<>(record, v)))
                .when(spyStore).getConfigurationRecord(args.get(0), args.get(1), null, null);

        faults = update.check(spyStore, null);

        Assert.assertTrue("Test for if available",
                outputFaults(faults).contains("StreamConfigurationRecord consistency check requires human intervention"));
    }

    @Test
    public void testScaleCheck() throws Exception {
        List<String> args = new ArrayList<>();
        args.add("test-scope");
        args.add("test-stream");
        Version v = mock(Version.class);

        ScaleCheckCommand scale = new ScaleCheckCommand(new CommandArgs(args, STATE.get()));

        ExtendedStreamMetadataStore spyStore = spy(ExtendedStreamMetadataStore.class);
        Map<Record, Set<Fault>> faults;

        // Test 1: EpochTransitionRecord is corrupted or unavailable.
        doReturn(Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "no truncation record found")))
                .when(spyStore).getEpochTransition(args.get(0), args.get(1), null, null);

        faults = scale.check(spyStore, null);

        Assert.assertTrue("Test for if unavailable",
                outputFaults(faults).contains("EpochTransitionRecord is corrupted or unavailable"));

        // Test 2: EpochTransitionRecord is available but EMPTY
        doReturn(CompletableFuture.completedFuture(new VersionedMetadata<>(EpochTransitionRecord.EMPTY, v)))
                .when(spyStore).getEpochTransition(args.get(0), args.get(1), null, null);

        faults = scale.check(spyStore, null);

        Assert.assertTrue("Test for if EMPTY", faults.isEmpty());

        // Test 3: EpochTransitionRecord is available
    }

    @Test
    public void testCommittingTransactionsCheck() throws Exception {
        List<String> args = new ArrayList<>();
        args.add("test-scope");
        args.add("test-stream");
        Version v = mock(Version.class);

        CommittingTransactionsCheckCommand committingTxn = new CommittingTransactionsCheckCommand(new CommandArgs(args, STATE.get()));

        ExtendedStreamMetadataStore spyStore = spy(ExtendedStreamMetadataStore.class);
        Map<Record, Set<Fault>> faults;

        // Test 1: CommittingTransactionsRecord is corrupted or unavailable
        doReturn(Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "no truncation record found")))
                .when(spyStore).getVersionedCommittingTransactionsRecord(args.get(0), args.get(1), null, null);

        faults = committingTxn.check(spyStore, null);

        Assert.assertTrue("Test for if unavailable",
                outputFaults(faults).contains("CommittingTransactionsRecord is corrupted or unavailable"));

        // Test 2: CommittingTransactionsRecord is available but not rolling
    }

    @Test
    public void testTruncateCheck() throws Exception {
        List<String> args = new ArrayList<>();
        args.add("test-scope");
        args.add("test-stream");
        Version v = mock(Version.class);

        TruncateCheckCommand truncate = new TruncateCheckCommand(new CommandArgs(args, STATE.get()));

        ExtendedStreamMetadataStore spyStore = spy(ExtendedStreamMetadataStore.class);
        Map<Record, Set<Fault>> faults;

        // Test 1: StreamTruncationRecord is corrupted or unavailable.
        doReturn(Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "no truncation record found")))
                .when(spyStore).getTruncationRecord(args.get(0), args.get(1), null, null);

        faults = truncate.check(spyStore, null);

        Assert.assertTrue("Test for if unavailable",
                outputFaults(faults).contains("StreamTruncationRecord is corrupted or unavailable"));

        // Test 2: StreamTruncationRecord is available but EMPTY
        doReturn(CompletableFuture.completedFuture(new VersionedMetadata<>(StreamTruncationRecord.EMPTY, v)))
                .when(spyStore).getTruncationRecord(args.get(0), args.get(1), null, null);

        faults = truncate.check(spyStore, null);

        Assert.assertTrue("Test for if EMPTY", faults.isEmpty());

        // Test 3: StreamTruncationRecord is available

    }
}
