package io.pravega.tools.pravegacli.integarationTest.troubleshoot;

import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.*;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.troubleshoot.UpdateCheckCommand;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class UpdateCheckCommandTest {
    // Setup utility.
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
    private UpdateCheckCommand updatecheck;
    private  String tablename;
    private String testStream ;

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
        updatecheck= new UpdateCheckCommand(commandArgs);
        serviceConfig = commandArgs.getState().getConfigBuilder().build().getConfig(ServiceConfig::builder);
        executor = commandArgs.getState().getExecutor();
    }

    @Test
    public void executeCommand() throws Exception {
        testStream="testStream";
        SETUP_UTILS.createTestStream(testStream,1);
        initialsetup_commands();
        initialsetup_store();
        tablename = SETUP_UTILS.getMetadataTable(testStream,storeHelper).join();
        VersionedMetadata<StreamConfigurationRecord> currentstreamEpochVersionMetadata = storeHelper.getEntry(tablename, "configuration", x -> StreamConfigurationRecord.fromBytes(x)).get();
        Version version=currentstreamEpochVersionMetadata.getVersion();
        //removing the configuration record
        storeHelper.removeEntry(tablename,"configuration",version).join();
        StreamMetadataStore mystore = SETUP_UTILS.createMetadataStore(executor,serviceConfig,commandArgs);
        //checking for update fault
        String result = SETUP_UTILS.faultvalue(updatecheck.check(mystore, executor));
        Assert.assertTrue("StreamConfigurationRecord is corrupted or unavailable".equalsIgnoreCase(result));
    }



}
