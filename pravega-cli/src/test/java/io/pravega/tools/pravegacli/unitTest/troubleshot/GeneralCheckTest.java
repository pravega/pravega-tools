package io.pravega.tools.pravegacli.unitTest.troubleshot;

import com.google.common.collect.ImmutableList;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.PravegaTablesStoreHelper;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.Version;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.troubleshoot.Fault;
import io.pravega.tools.pravegacli.commands.troubleshoot.GeneralCheckCommand;
import io.pravega.tools.pravegacli.commands.troubleshoot.Record;
import io.pravega.tools.pravegacli.integarationTest.troubleshoot.ToolSetupUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class GeneralCheckTest {
    // Setup utility.
    private Map<Record, Set<Fault>> faults;
    private static final ToolSetupUtils SETUP_UTILS = new ToolSetupUtils();
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();
    private ServiceConfig serviceConfig;
    private CommandArgs commandArgs;
    private volatile StreamMetadataStore store;
    private ScheduledExecutorService executor;
    private GeneralCheckCommand genralCheck;
    private String testStream ;
    StreamMetadataStore storeMock;

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
        genralCheck = new GeneralCheckCommand(commandArgs);
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

        //mocking the store for the ut's
        storeMock= Mockito.mock(StreamMetadataStore.class);

        //testing for reference missmatch
        String result =checkingReferenceEpochMissmatch();
        Assert.assertTrue(result.equalsIgnoreCase("Reference epoch mismatch."));

        //testing for epoch missmatch
        String result2 =checkingEpochMissmatch();
        Assert.assertTrue(result2.equalsIgnoreCase("Epoch mismatch : May or may not be the correct record."));

        //testing for creation_time missmatch
        String result3 =checkingCreationTimeMissmatch();
        Assert.assertTrue(result3.equalsIgnoreCase("Creation time mismatch."));

        //testing for Segments missmatch
        String result4 =checkingSegmentsMissmatch();
        Assert.assertTrue(result4.equalsIgnoreCase("Segment data mismatch."));

    }


    public String checkingReferenceEpochMissmatch()
    {
        EpochRecord ep= store.getEpoch("scope",testStream,0,null,executor).join();
        EpochRecord nep=new EpochRecord(ep.getEpoch(),4,ep.getSegments(),ep.getCreationTime());
        Mockito.when(storeMock.
                getEpoch("scope", testStream, 0, null, executor)).
                thenReturn(CompletableFuture.completedFuture(nep));
        Mockito.when(storeMock.
                getHistoryTimeSeriesChunk("scope", testStream, 0, null , executor)).
                thenReturn(store.getHistoryTimeSeriesChunk("scope", testStream, 0, null , executor));
        Mockito.when(storeMock
                .getActiveEpoch("scope", testStream, null, true, executor)).
                thenReturn(CompletableFuture.completedFuture(nep));
        faults = genralCheck.check(storeMock, executor);
        return (SETUP_UTILS.faultvalue(faults));
    }

    public String checkingEpochMissmatch()
    {
        EpochRecord ep= store.getEpoch("scope",testStream,0,null,executor).join();
        EpochRecord nep=new EpochRecord(4,ep.getReferenceEpoch(),ep.getSegments(),ep.getCreationTime());
         Mockito.when(storeMock.
                getEpoch("scope", testStream, 0, null, executor)).
                thenReturn(CompletableFuture.completedFuture(nep));
        Mockito.when(storeMock.
                getHistoryTimeSeriesChunk("scope", testStream, 0, null , executor)).
                thenReturn(store.getHistoryTimeSeriesChunk("scope", testStream, 0, null , executor));
        Mockito.when(storeMock.
                getActiveEpoch("scope", testStream, null, true, executor)).
                thenReturn(CompletableFuture.completedFuture(nep));
        faults = genralCheck.check(storeMock, executor);
        return (SETUP_UTILS.faultvalue(faults));
    }

    public String checkingCreationTimeMissmatch()
    {
        EpochRecord ep= store.getEpoch("scope",testStream,0,null,executor).join();
        EpochRecord nep=new EpochRecord(ep.getEpoch(),ep.getReferenceEpoch(),ep.getSegments(),3);
        Mockito.when(storeMock.
                getEpoch("scope", testStream, 0, null, executor)).
                thenReturn(CompletableFuture.completedFuture(nep));
        Mockito.when(storeMock.
                getHistoryTimeSeriesChunk("scope", testStream, 0, null , executor)).
                thenReturn(store.getHistoryTimeSeriesChunk("scope", testStream, 0, null , executor));
        Mockito.when(storeMock
                .getActiveEpoch("scope", testStream, null, true, executor)).
                thenReturn(CompletableFuture.completedFuture(nep));
        faults = genralCheck.check(storeMock, executor);
        return (SETUP_UTILS.faultvalue(faults));
    }

    public String checkingSegmentsMissmatch()
    {
        EpochRecord ep= store.getEpoch("scope",testStream,0,null,executor).join();
        List<StreamSegmentRecord> currentSegmentsList =ep.getSegments();
        StreamSegmentRecord currentStreamSegmentRecord=currentSegmentsList.get(0);
        long creationTime = store.getEpoch("scope",testStream,0,null,executor).join().getCreationTime();
        StreamSegmentRecord newStreamSegmentRecord=new StreamSegmentRecord(4, currentStreamSegmentRecord.getCreationEpoch(),creationTime,currentStreamSegmentRecord.getKeyStart(),currentStreamSegmentRecord.getKeyEnd());
        List<StreamSegmentRecord> newSegmentsList = new ArrayList<>();
        newSegmentsList.add(newStreamSegmentRecord);
        ImmutableList<StreamSegmentRecord> immutableList = ImmutableList.copyOf(newSegmentsList);
        EpochRecord nep=new EpochRecord(ep.getEpoch(),ep.getReferenceEpoch(),immutableList,ep.getCreationTime());
        Mockito.when(storeMock.
                getEpoch("scope", testStream, 0, null, executor)).
                thenReturn(CompletableFuture.completedFuture(nep));
        Mockito.when(storeMock.
                getHistoryTimeSeriesChunk("scope", testStream, 0, null , executor)).
                thenReturn(store.getHistoryTimeSeriesChunk("scope", testStream, 0, null , executor));
        Mockito.when(storeMock
                .getActiveEpoch("scope", testStream, null, true, executor)).
                thenReturn(CompletableFuture.completedFuture(nep));
        faults = genralCheck.check(storeMock, executor);
        return (SETUP_UTILS.faultvalue(faults));
    }

}
