package io.pravega.tools.pravegacli.unitTest.troubleshot;

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
import io.pravega.tools.pravegacli.integarationTest.troubleshoot.ToolSetupUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class TruncateCheckTest {
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
    private TruncateCheckCommand tc;
    private  String tablename;
    private String testStream ;
    private Map<Record, Set<Fault>> faults;
    private StreamMetadataStore mockStore;

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
        tc= new TruncateCheckCommand(commandArgs);
        serviceConfig = commandArgs.getState().getConfigBuilder().build().getConfig(ServiceConfig::builder);
        executor = commandArgs.getState().getExecutor();

    }
    @Test
    public void executeCommand() throws Exception {
        testStream = "testStream";
        String stream="testStream";
        String scope="scope";
        initialsetup_commands();
        initialsetup_store();
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();
        SETUP_UTILS.createTestStream_withconfig(testStream, 1,configuration);
        tablename = SETUP_UTILS.getMetadataTable(testStream,storeHelper).join();

        store.setState(scope, stream, State.ACTIVE, null, executor).get();
        //mocking the store
        mockStore=Mockito.mock(StreamMetadataStore.class);

        //checking for unavailability
        VersionedMetadata<StreamTruncationRecord> currentStreamTruncationRecord1 = storeHelper.getEntry(tablename, "truncation", x -> StreamTruncationRecord.fromBytes(x)).get();
        String result1=unavailability_check(currentStreamTruncationRecord1);
        Assert.assertTrue(result1.equalsIgnoreCase("StreamTruncationRecord is corrupted or unavailable"));

        //checking for inconsistency
        String result2=inconsistency_check();
        Assert.assertTrue(result2.equalsIgnoreCase("StreamTruncationRecord inconsistency in regards to updating and segments to delete"));

        //check for segment count inconsistency
        String result3=segment_count_inconsistency_check();
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
    private String inconsistency_check()
    {
        ImmutableSet<Long> toDelete=ImmutableSet.of(4L,5L);
        StreamTruncationRecord streamTruncationRecord=store.getTruncationRecord("scope", testStream, null, executor).join().getObject();
        StreamTruncationRecord newstreamTruncationRecord=new StreamTruncationRecord(streamTruncationRecord.getStreamCut(),streamTruncationRecord.getSpan(),streamTruncationRecord.getDeletedSegments(),toDelete,streamTruncationRecord.getSizeTill(),streamTruncationRecord.isUpdating());
        Version.IntVersion ver = Version.IntVersion.builder().intValue(0).build();
        VersionedMetadata<StreamTruncationRecord> mockVersionRecord=new VersionedMetadata<>(newstreamTruncationRecord,ver);
        Mockito.when(mockStore.getTruncationRecord("scope", testStream, null,executor)).thenReturn(CompletableFuture.completedFuture(mockVersionRecord));
        faults = tc.check(mockStore, executor);
        return(SETUP_UTILS.faultvalue(faults));
    }
    private String segment_count_inconsistency_check()
    {
    ImmutableSet<Long> toDelete=ImmutableSet.of(4L,5L);
    StreamTruncationRecord streamTruncationRecord=store.getTruncationRecord("scope", testStream, null, executor).join().getObject();
    StreamTruncationRecord newstreamTruncationRecord=new StreamTruncationRecord(streamTruncationRecord.getStreamCut(),streamTruncationRecord.getSpan(),streamTruncationRecord.getDeletedSegments(),toDelete,streamTruncationRecord.getSizeTill(),true);
    Version.IntVersion ver = Version.IntVersion.builder().intValue(0).build();
    VersionedMetadata<StreamTruncationRecord> mockVersionRecord=new VersionedMetadata<>(newstreamTruncationRecord,ver);
    Mockito.when(mockStore.getTruncationRecord("scope", testStream, null,executor)).thenReturn(CompletableFuture.completedFuture(mockVersionRecord));
    faults = tc.check(mockStore, executor);
    return(SETUP_UTILS.faultvalue(faults));
    }
}
