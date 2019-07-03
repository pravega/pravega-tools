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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.ExtendedStreamMetadataStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.Version;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.*;
import io.pravega.test.integration.utils.SetupUtils;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.troubleshoot.CommittingTransactionsCheckCommand;
import io.pravega.tools.pravegacli.commands.troubleshoot.Fault;
import io.pravega.tools.pravegacli.commands.troubleshoot.GeneralCheckCommand;
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

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.controller.store.stream.records.HistoryTimeSeries.addHistoryRecord;
import static io.pravega.controller.store.stream.records.StreamSegmentRecord.newSegmentRecord;
import static io.pravega.tools.pravegacli.commands.utils.OutputUtils.outputFaults;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
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
        List<String> args = new ArrayList<>();
        args.add("test-scope");
        args.add("test-stream");
        Version v = mock(Version.class);

        GeneralCheckCommand general = new GeneralCheckCommand(new CommandArgs(args, STATE.get()));

        ExtendedStreamMetadataStore spyStore = spy(ExtendedStreamMetadataStore.class);
        Map<Record, Set<Fault>> faults;

        // Test 1: HistoryTimeSeries is corrupted or unavailable.
        doReturn(Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "no history found")))
                .when(spyStore).getHistoryTimeSeriesChunkRecent(args.get(0), args.get(1), null, null);

        faults = general.check(spyStore, null);

        Assert.assertTrue("Test for is unavailable",
                outputFaults(faults).contains("HistoryTimeSeries chunk is corrupted or unavailable"));

        // Test 2: HistoryTimeSeries is available.
        HistoryTimeSeries spyHistory = spy(new HistoryTimeSeries(ImmutableList.of()));

        doReturn(CompletableFuture.completedFuture(spyHistory))
                .when(spyStore).getHistoryTimeSeriesChunkRecent(args.get(0), args.get(1), null, null);

        // Test 2.1: HistoryTimeSeriesRecords are unavailable.
        doThrow(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "no history records found")).when(spyHistory).getHistoryRecords();

        faults = general.check(spyStore, null);

        Assert.assertTrue("Test for history records unavailable",
                outputFaults(faults).contains("HistoryTimeSeries is missing history records."));

        // Test 2.2: HistoryTimeSeriesRecords are available.
        int epochZero = 0;
        int epochOne = 1;
        ImmutableList<StreamSegmentRecord> segmentsCreated = ImmutableList.of(newSegmentRecord(1, 1, 10L, 0.0, 0.5),
                newSegmentRecord(2, 1, 10L, 0.5, 1.0));
        ImmutableList<StreamSegmentRecord> segmentsSealed = ImmutableList.of(newSegmentRecord(0, 0, 1L, 0, 1));

        EpochRecord spyEpochZero = spy(new EpochRecord(epochZero, epochZero, segmentsSealed, 6L));
        HistoryTimeSeriesRecord spyHistoryRecordZero = spy(new HistoryTimeSeriesRecord(epochZero, epochZero, ImmutableList.of(), segmentsSealed, 9L));

        EpochRecord spyEpochOne = spy(new EpochRecord(epochOne, epochOne, segmentsCreated, 10L));
        HistoryTimeSeriesRecord spyHistoryRecordOne = spy(new HistoryTimeSeriesRecord(epochOne, epochOne, segmentsSealed, segmentsCreated, 10L));

        addHistoryRecord(spyHistory, spyHistoryRecordZero);
        addHistoryRecord(spyHistory, spyHistoryRecordOne);
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

        // Test 3: EpochTransitionRecord is available.
        int epoch = 1;
        ImmutableList<StreamSegmentRecord> segmentsCreated = ImmutableList.of(newSegmentRecord(1, 1, 10L, 0.0, 0.5),
                newSegmentRecord(2, 1, 10L, 0.5, 1.0));
        ImmutableList<StreamSegmentRecord> segmentsSealed = ImmutableList.of(newSegmentRecord(0, 0, 1L, 0, 1));

        EpochTransitionRecord spyRecord = spy(EpochTransitionRecord.EMPTY);
        EpochRecord spyEpoch = spy(new EpochRecord(1, 1, segmentsCreated, 10L));
        HistoryTimeSeriesRecord spyHistory = spy(new HistoryTimeSeriesRecord(1, 1, segmentsSealed, segmentsCreated, 10L));

        doReturn(CompletableFuture.completedFuture(new VersionedMetadata<>(spyRecord, v)))
                .when(spyStore).getEpochTransition(args.get(0), args.get(1), null, null);

        // Test 3.1: EpochTransitionRecord new epoch field unavailable.
        doThrow(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "Transition record's new epoch not found")).when(spyRecord).getNewEpoch();

        faults = scale.check(spyStore, null);
        Assert.assertTrue("Test for new epoch unavailable",
                outputFaults(faults).contains("EpochTransitionRecord is missing new epoch."));

        // Test 3.2: The corresponding EpochRecord and HistoryTimeSeriesRecord are unavailable.
        doReturn(epoch).when(spyRecord).getNewEpoch();
        doReturn(Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "no epoch record found"))).when(spyStore).getEpoch(args.get(0), args.get(1), epoch, null, null);
        doReturn(Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "no history record found"))).when(spyStore).getHistoryTimeSeriesRecord(args.get(0), args.get(1), epoch, null, null);

        faults = scale.check(spyStore, null);
        Assert.assertTrue("Test for corresponding EpochRecord unavailability",
                outputFaults(faults).contains("Epoch: "+ epoch + ", The corresponding EpochRecord is corrupted or does not exist."));
        Assert.assertTrue("Test for corresponding HistoryTimeSeriesRecord unavailability",
                outputFaults(faults).contains("History: "+ epoch + ", The corresponding HistoryTimeSeriesRecord is corrupted or does not exist."));

        // Test 3.3: The corresponding EpochRecord and HistoryTimeSeriesRecord are available.
        doReturn(CompletableFuture.completedFuture(spyEpoch)).when(spyStore)
                .getEpoch(args.get(0), args.get(1), epoch, null, null);
        doReturn(CompletableFuture.completedFuture(spyHistory)).when(spyStore)
                .getHistoryTimeSeriesRecord(args.get(0), args.get(1), epoch, null, null);

        doReturn(CompletableFuture.completedFuture(true)).when(spyStore).checkSegmentSealed(args.get(0), args.get(1), 0, null, null);

        // Test 3.3.1: The required EpochTransitionRecord fields are unavailable.
        doThrow(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "Transition record's new segments not found")).when(spyRecord).getNewSegmentsWithRange();
        doThrow(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "Transition record's sealed segments not found")).when(spyRecord).getSegmentsToSeal();

        faults = scale.check(spyStore, null);
        Assert.assertTrue("Test for segments created unavailable",
                outputFaults(faults).contains("EpochTransitionRecord is missing segments created."));
        Assert.assertTrue("Test for segments to be sealed unavailable",
                outputFaults(faults).contains("EpochTransitionRecord is missing segments to be sealed."));

        // Test 3.3.2: The required EpochTransitionRecord fields are available.
        // Test 3.3.2.1: Inconsistency case.
        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.0, 0.6);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.6, 1.0);
        ImmutableMap<Long, Map.Entry<Double, Double>> newSegments = ImmutableMap.of(1L, segment1, 2L, segment2);
        ImmutableSet<Long> sealedSegments = ImmutableSet.of(1L);

        doReturn(newSegments).when(spyRecord).getNewSegmentsWithRange();
        doReturn(sealedSegments).when(spyRecord).getSegmentsToSeal();

        faults = scale.check(spyStore, null);
        Assert.assertTrue("Test for inconsistency between newRanges and segments created",
                outputFaults(faults).contains("EpochRecord and the EpochTransitionRecord mismatch in the segments"));
        Assert.assertTrue("Test for inconsistency between segmentsToSeal and segmentsSealed",
                outputFaults(faults).contains("HistoryTimeSeriesRecord and EpochTransitionRecord mismatch in the sealed segments"));

        // Test 3.3.2.2: Consistency case.
        segment1 = new SimpleEntry<>(0.0, 0.5);
        segment2 = new SimpleEntry<>(0.5, 1.0);
        newSegments = ImmutableMap.of(1L, segment1, 2L, segment2);
        sealedSegments = ImmutableSet.of(0L);

        doReturn(newSegments).when(spyRecord).getNewSegmentsWithRange();
        doReturn(sealedSegments).when(spyRecord).getSegmentsToSeal();

        faults = scale.check(spyStore, null);
        Assert.assertTrue("Test for no inconsistencies", faults.isEmpty());
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
        StreamTruncationRecord spyRecord = spy(StreamTruncationRecord.EMPTY);

        doReturn(CompletableFuture.completedFuture(new VersionedMetadata<>(spyRecord, v)))
                .when(spyStore).getTruncationRecord(args.get(0), args.get(1), null, null);

        // Test 3.1: If the fields are unavailable.
        doThrow(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "Truncation record's updating not found")).when(spyRecord).isUpdating();
        doThrow(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "Truncation record's to delete segments not found")).when(spyRecord).getToDelete();
        doThrow(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "Truncation record's deleted segments not found")).when(spyRecord).getDeletedSegments();
        doThrow(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "Truncation record's stream cut not found")).when(spyRecord).getStreamCut();

        faults = truncate.check(spyStore, null);
        Assert.assertTrue("Test for updating unavailable",
                outputFaults(faults).contains("StreamTruncationRecord is missing updating."));
        Assert.assertTrue("Test for segments to delete unavailable",
                outputFaults(faults).contains("StreamTruncationRecord is missing segments to delete."));
        Assert.assertTrue("Test for deleted segments unavailable",
                outputFaults(faults).contains("StreamTruncationRecord is missing deleted segments."));
        Assert.assertTrue("Test for stream cut unavailable",
                outputFaults(faults).contains("StreamTruncationRecord is missing stream cut."));

        // Test 3.2: If fields are available.
        // Test 3.2.1: If fields are available with inconsistencies.
        ImmutableMap<Long, Long> streamCut = ImmutableMap.of(3L, 10L, 4L, 100L);
        ImmutableSet<Long> toDelete = ImmutableSet.of(5L);
        ImmutableSet<Long> deleted = ImmutableSet.of(2L);

        doReturn(false).when(spyRecord).isUpdating();
        doReturn(toDelete).when(spyRecord).getToDelete();
        doReturn(deleted).when(spyRecord).getDeletedSegments();
        doReturn(streamCut).when(spyRecord).getStreamCut();

        faults = truncate.check(spyStore, null);
        Assert.assertTrue("Test for inconsistency between updating and toDelete",
                outputFaults(faults).contains("StreamTruncationRecord inconsistency in regards to updating and segments to delete"));
        Assert.assertTrue("Test for between stream cut and deletion related segments",
                outputFaults(faults).contains("Fault in the StreamTruncationRecord in regards to segments deletion, segments ahead of stream cut being deleted"));

        // Test 3.2.2: If fields are available without inconsistencies.
        streamCut = ImmutableMap.of(3L, 10L, 4L, 100L);
        toDelete = ImmutableSet.of(1L);
        deleted = ImmutableSet.of(2L);

        doReturn(true).when(spyRecord).isUpdating();
        doReturn(toDelete).when(spyRecord).getToDelete();
        doReturn(deleted).when(spyRecord).getDeletedSegments();
        doReturn(streamCut).when(spyRecord).getStreamCut();

        faults = truncate.check(spyStore, null);
        Assert.assertTrue("Test for no inconsistencies", faults.isEmpty());
    }
}
