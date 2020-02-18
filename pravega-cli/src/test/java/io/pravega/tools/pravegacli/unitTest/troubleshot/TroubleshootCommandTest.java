package io.pravega.tools.pravegacli.unitTest.troubleshot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.*;
import io.pravega.controller.store.stream.records.*;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.troubleshoot.Fault;
import io.pravega.tools.pravegacli.commands.troubleshoot.Record;
import io.pravega.tools.pravegacli.commands.troubleshoot.TroubleshootCheckCommand;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TroubleshootCommandTest {
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
    private TroubleshootCheckCommand tc;
    private String testStream ;
    private  String tablename;
    private String scope,stream;

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

    public void initialsetup_commands(String testStream)
    {
        commandArgs = new CommandArgs(Arrays.asList(SETUP_UTILS.getScope(), testStream), STATE.get());
        tc = new TroubleshootCheckCommand(commandArgs);
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
        scope="scope";
        stream="test";
        testStream = "testStream";
        SETUP_UTILS.createTestStream(testStream, 1);
        initialsetup_commands(testStream);
        initialsetup_store();
        StreamMetadataStore mystoremock = Mockito.mock(StreamMetadataStore.class);
        EpochTransitionRecord epRecord=store.getEpochTransition("scope",testStream,null,executor).join().getObject();
        System.out.println("value of epoch = "+epRecord);

        //checking for update Failure
        String result1=update_check(mystoremock);
        Assert.assertTrue(result1.contains("StreamConfigurationRecord consistency check requires human intervention"));

        //checking for general Faliure
        String result2=general_check(mystoremock);
        Assert.assertTrue(result2.contains("Reference epoch mismatch."));

        //checking for truncate Error
        String result3=truncate_check(mystoremock);
        Assert.assertTrue(result3.contains("StreamTruncationRecord inconsistency in regards to updating and segments to delete"));

        //checking for scale Error
        String result4=scale_check(mystoremock);
        Assert.assertEquals(result4,"Epoch mismatch : May or may not be the correct record.");

        //checking for commiting Error
        String result5 = commiting_check(mystoremock);
        Assert.assertEquals(result5,"Duplicate txn epoch: 2 is corrupted or does not exist.");

        //if there's no Error
        String result6 = tc.check(mystoremock,executor);
        Assert.assertEquals(result6,"Everything seems OK.");

    }

    public String update_check(StreamMetadataStore mystoremock)
    {
        //checking for fault if configurationRecord is null
        String result = tc.check(mystoremock, executor);

        //returning to orignal state
        StreamConfigurationRecord presentStreamConfigurationRecord= store.getConfigurationRecord("scope",testStream,
                null,executor).join().getObject();
        StreamConfigurationRecord mockStreamConfigurationRecord=new StreamConfigurationRecord(presentStreamConfigurationRecord.getScope(),
                presentStreamConfigurationRecord.getStreamName(),presentStreamConfigurationRecord.getStreamConfiguration(),true);
        Version.IntVersion ver = Version.IntVersion.builder().intValue(0).build();
        VersionedMetadata<StreamConfigurationRecord> mockVersionRecord=new VersionedMetadata<>(mockStreamConfigurationRecord,ver);
        Mockito.when(mystoremock.getConfigurationRecord("scope",testStream,null,executor)).thenReturn(CompletableFuture.completedFuture(mockVersionRecord));

        return result;
    }
    public String general_check(StreamMetadataStore mystoremock)
    {
        EpochRecord ep= store.getEpoch("scope",testStream,0,null,executor).join();
        EpochRecord nep=new EpochRecord(ep.getEpoch(),4,ep.getSegments(),ep.getCreationTime());
        Mockito.when(mystoremock.getEpoch("scope", testStream, 0, null, executor)).
                thenReturn(CompletableFuture.completedFuture(nep));
        Mockito.when(mystoremock.getHistoryTimeSeriesChunk("scope", testStream, 0, null , executor)).
                thenReturn(store.getHistoryTimeSeriesChunk("scope", testStream, 0, null , executor));
        Mockito.when(mystoremock.getActiveEpoch("scope", testStream, null, true, executor)).
                thenReturn(CompletableFuture.completedFuture(nep));
       String result =tc.check(mystoremock, executor);

       //returning to orignal state
        Mockito.when(mystoremock.getEpoch("scope", testStream, 0, null, executor)).
                thenReturn(CompletableFuture.completedFuture(ep));
        return result;
    }
    public String truncate_check(StreamMetadataStore mystoremock)
    {
        ImmutableSet<Long> toDelete=ImmutableSet.of(4L,5L);
        StreamTruncationRecord streamTruncationRecord=store.getTruncationRecord("scope", testStream, null, executor).join().getObject();
        StreamTruncationRecord newstreamTruncationRecord=new StreamTruncationRecord(streamTruncationRecord.getStreamCut(),streamTruncationRecord.getSpan(),streamTruncationRecord.getDeletedSegments(),toDelete,streamTruncationRecord.getSizeTill(),streamTruncationRecord.isUpdating());
        Version.IntVersion ver = Version.IntVersion.builder().intValue(0).build();
        VersionedMetadata<StreamTruncationRecord> mockVersionRecord=new VersionedMetadata<>(newstreamTruncationRecord,ver);
        Mockito.when(mystoremock.getTruncationRecord("scope", testStream, null,executor)).thenReturn(CompletableFuture.completedFuture(mockVersionRecord));
        String result= tc.check(mystoremock, executor);

        //converting to old state
        mockVersionRecord=new VersionedMetadata<>(streamTruncationRecord,ver);
        Mockito.when(mystoremock.getTruncationRecord("scope", testStream, null,executor)).
                thenReturn(CompletableFuture.completedFuture(mockVersionRecord));

        return(result);
    }

    public VersionedMetadata<EpochTransitionRecord> do_scale()
    {
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
    public String scale_check(StreamMetadataStore mystoremock)
    {
        initialsetup_commands(stream);
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();
        long start = System.currentTimeMillis();
        store.createStream(scope, stream, configuration, start, null, executor).join();
        store.setState(scope, stream, State.ACTIVE, null, executor).join();
        tablename = SETUP_UTILS.getMetadataTable(stream,storeHelper).join();

        VersionedMetadata<EpochTransitionRecord> currentEpochTransitionRecordMetadata=do_scale();
        VersionedMetadata<EpochTransitionRecord> currentEpochTransitionRecordMetadata1 = storeHelper.getEntry(tablename, "epochTransition", x -> EpochTransitionRecord.fromBytes(x)).join();
        Version version = currentEpochTransitionRecordMetadata1.getVersion();
        storeHelper.removeEntry(tablename, "epochTransition", version).join();
        storeHelper.addNewEntry(tablename, "epochTransition", currentEpochTransitionRecordMetadata.getObject().toBytes()).join();

        StreamConfigurationRecord presentStreamConfigurationRecord= store.getConfigurationRecord("scope",testStream,
                null,executor).join().getObject();
        StreamConfigurationRecord mockStreamConfigurationRecord=new StreamConfigurationRecord(presentStreamConfigurationRecord.getScope(),
                presentStreamConfigurationRecord.getStreamName(),presentStreamConfigurationRecord.getStreamConfiguration(),true);
        Version.IntVersion v = Version.IntVersion.builder().intValue(0).build();
        VersionedMetadata<StreamConfigurationRecord> mockconfigVersionRecord=new VersionedMetadata<>(mockStreamConfigurationRecord,v);
        Mockito.when(mystoremock.getConfigurationRecord("scope",stream,null,executor)).thenReturn(CompletableFuture.completedFuture(mockconfigVersionRecord));

        VersionedMetadata<EpochTransitionRecord> currentEpochTransitionRecordMetadata2= storeHelper.getEntry(tablename, "epochTransition", x -> EpochTransitionRecord.fromBytes(x)).join();
        EpochTransitionRecord currentEpochTransitionRecord=currentEpochTransitionRecordMetadata2.getObject();
        EpochRecord currentEpochRecord = store.getEpoch(scope, stream, currentEpochTransitionRecord.getNewEpoch(),
                null, executor).join();

        int chunkNumber=currentEpochTransitionRecord.getNewEpoch()/ HistoryTimeSeries.HISTORY_CHUNK_SIZE;

        HistoryTimeSeriesRecord currentHistoryRecord = store.getHistoryTimeSeriesChunk(scope, stream, chunkNumber,
                null, executor).join().getLatestRecord();

        EpochTransitionRecord newEpochTransitionRecord=new EpochTransitionRecord(currentEpochTransitionRecord.getActiveEpoch(),currentEpochTransitionRecord.getTime()
                ,currentEpochTransitionRecord.getSegmentsToSeal(),currentEpochTransitionRecord.getNewSegmentsWithRange());

        EpochRecord newEpochRecord=new EpochRecord(4,currentEpochRecord.getReferenceEpoch(),currentEpochRecord.getSegments()
                ,currentEpochRecord.getCreationTime());

        HistoryTimeSeriesRecord newHistoryTimeSeriesRecord=new HistoryTimeSeriesRecord(currentHistoryRecord.getEpoch(),
                currentHistoryRecord.getReferenceEpoch(),currentHistoryRecord.getSegmentsSealed(),
                currentHistoryRecord.getSegmentsCreated(),currentHistoryRecord.getScaleTime());

        Version.IntVersion ver = Version.IntVersion.builder().intValue(0).build();
        VersionedMetadata<EpochTransitionRecord> mockVersionRecord=new VersionedMetadata<>(newEpochTransitionRecord,ver);

        Mockito.when( mystoremock.getEpochTransition(scope, stream, null, executor)).
                thenReturn(CompletableFuture.completedFuture(mockVersionRecord));

        Mockito.when(mystoremock.getEpoch(scope,stream,currentEpochTransitionRecord.getNewEpoch()
                ,null,executor)).thenReturn(CompletableFuture.completedFuture(newEpochRecord));

        Mockito.when( mystoremock.getHistoryTimeSeriesChunk(scope, stream, chunkNumber,
                null, executor)).thenReturn(store.getHistoryTimeSeriesChunk(scope,testStream,
                chunkNumber,null,executor));

        Mockito.when(mystoremock.getSegmentSealedEpoch("scope", stream, 1, null, executor)).
                thenReturn(CompletableFuture.completedFuture(0));


        Mockito.when(mystoremock.getEpoch("scope",stream,currentEpochTransitionRecord.getNewEpoch()
                ,null,executor)).thenReturn(CompletableFuture.completedFuture(newEpochRecord));

        Mockito.when(mystoremock.getActiveEpoch(scope, stream, null, true, executor)).
                thenReturn(CompletableFuture.completedFuture(newEpochRecord));

        int currentEpoch = store.getActiveEpoch(scope, stream, null, true, executor).join().getEpoch();
        int historyCurrentEpoch = store.getHistoryTimeSeriesChunk(scope, stream, (currentEpoch/ HistoryTimeSeries.HISTORY_CHUNK_SIZE),null, executor).join().getLatestRecord().getEpoch();

        System.out.println("value  of history current = "+historyCurrentEpoch);

        HistoryTimeSeries ht=store.getHistoryTimeSeriesChunk(scope, stream, (currentEpoch/ HistoryTimeSeries.HISTORY_CHUNK_SIZE),null, executor).join();

        Mockito.when(mystoremock.getHistoryTimeSeriesChunk(scope, stream, (currentEpoch/ HistoryTimeSeries.HISTORY_CHUNK_SIZE),null, executor)).
                thenReturn(CompletableFuture.completedFuture(ht));

        String result=tc.check(mystoremock,executor);

        //changing back to orignal
        Mockito.when(mystoremock.getEpoch("scope",stream,currentEpochTransitionRecord.getNewEpoch()
                ,null,executor)).thenReturn(CompletableFuture.completedFuture(currentEpochRecord));
        return result;
    }
    public String commiting_check(StreamMetadataStore mockstore)
    {
        testStream="test";
        UUID v1 = new UUID(0, 1);
        UUID v2 = new UUID(1, 2);
        ImmutableList<UUID> list = ImmutableList.of(v1, v2);
        CommittingTransactionsRecord newTransactionRecord = new CommittingTransactionsRecord(0, list, 1);
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

        String result = tc.check(mockstore, executor);

        //changng back to orignal state
        Mockito.when(mockstore.getVersionedCommittingTransactionsRecord("scope", testStream, null, executor)).
                thenReturn(store.getVersionedCommittingTransactionsRecord("scope", testStream, null, executor));

        return result;
    }

}
