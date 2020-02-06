package io.pravega.tools.pravegacli.integarationTest;

import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.*;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.troubleshoot.Fault;
import io.pravega.tools.pravegacli.commands.troubleshoot.Record;
import io.pravega.tools.pravegacli.commands.troubleshoot.ScaleCheckCommand;
import io.pravega.tools.pravegacli.commands.troubleshoot.UpdateCheckCommand;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

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
        testStream = "testStream";
        SETUP_UTILS.createTestStream(testStream, 1);
        initialsetup_commands();
        initialsetup_store();
        tablename = SETUP_UTILS.getMetadataTable(testStream, storeHelper).join();


        //checking for unavailability of epochtransitionrecord
        VersionedMetadata<EpochTransitionRecord> currentEpochTransitionVersionMetadata = storeHelper.getEntry(tablename, "epochTransition", x -> EpochTransitionRecord.fromBytes(x)).get();
        String result=unvalabilityCheck(currentEpochTransitionVersionMetadata);
        System.out.println("value of fault = " + result);
        Assert.assertTrue("EpochTransitionRecord is corrupted or unavailable".equalsIgnoreCase(result));

        System.out.println("value = " +sc.check(store,executor));



    }

    public void changingBackToOrginalState(VersionedMetadata<EpochRecord> currentEpochVersionMetadata, EpochRecord oldEpoch )
    {
        Version version = currentEpochVersionMetadata.getVersion();
        EpochRecord currentEpoch=currentEpochVersionMetadata.getObject();
        storeHelper.removeEntry(tablename, "epochTransition", version).join();
        storeHelper.addNewEntry(tablename, "epochTransition", oldEpoch.toBytes()).join();

    }

    public String unvalabilityCheck( VersionedMetadata<EpochTransitionRecord> currentEpochTransitionVersionMetadata )
    {
        Version version = currentEpochTransitionVersionMetadata.getVersion();
        EpochTransitionRecord currentTransitionRecord=currentEpochTransitionVersionMetadata.getObject();
        storeHelper.removeEntry(tablename, "epochTransition", version).join();
        StreamMetadataStore mystore = SETUP_UTILS.createMetadataStore(executor,serviceConfig,commandArgs);
        faults = sc.check(mystore, executor);
        storeHelper.addNewEntry(tablename, "epochTransition",currentTransitionRecord.toBytes()).join();
        return (SETUP_UTILS.faultvalue(faults));
    }

}
