package io.pravega.tools.pravegacli.integarationTest;

import io.pravega.tools.pravegacli.commands.AdminCommandState;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class TroubleshootCheckCommandTest{
    // Setup utility.
    private static final ToolSetupUtils SETUP_UTILS = new ToolSetupUtils();
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    //@Rule
    //public final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

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

    @Test
    public void executeCommand() throws Exception {
        String testStream = "testStream";
        SETUP_UTILS.createTestStream(testStream, 1);
        String commandResult = TestUtils.executeCommand("troubleshoot diagnosis " +  SETUP_UTILS.getScope() + " testStream" , STATE.get());
        System.out.println( "result is = " +  commandResult);
        Assert.assertTrue(commandResult.contains("Everything seems OK"));

    }

}
