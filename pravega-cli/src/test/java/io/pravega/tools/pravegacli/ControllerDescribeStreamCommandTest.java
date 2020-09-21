/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli;

import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.common.Exceptions;
import io.pravega.common.cluster.Host;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.util.Config;
import io.pravega.local.LocalPravegaEmulator;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.controller.ControllerDescribeStreamCommand;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.Inet4Address;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ControllerDescribeStreamCommandTest {

    // Security related flags and instantiate local pravega server.
    private static final boolean REST_ENABLED = true;
    boolean AUTH_ENABLED = false;
    private static final Integer CONTROLLER_PORT = 9090;
    private static final Integer REST_SERVER_PORT = 9091;
    private static final Integer SEGMENT_STORE_PORT = 6000;
    LocalPravegaEmulator localPravega;
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    @Before
    public void setUp() throws Exception {

        // Create the secure pravega server to test commands against.
        LocalPravegaEmulator.LocalPravegaEmulatorBuilder emulatorBuilder = LocalPravegaEmulator.builder()
                .controllerPort(CONTROLLER_PORT)
                .segmentStorePort(SEGMENT_STORE_PORT)
                .zkPort(io.pravega.test.common.TestUtils.getAvailableListenPort())
                .restServerPort(io.pravega.test.common.TestUtils.getAvailableListenPort())
                .enableRestServer(REST_ENABLED)
                .enableAuth(AUTH_ENABLED)
                .enableTls(false)
                .restServerPort(REST_SERVER_PORT);

        // Since the server is being built right here, avoiding delegating these conditions to subclasses via factory
        // methods. This is so that it is easy to see the difference in server configs all in one place. This is also
        // unlike the ClientConfig preparation which is being delegated to factory methods to make their preparation
        // explicit in the respective test classes.

        if (AUTH_ENABLED) {
            emulatorBuilder.passwdFile("src/test/resources/passwd")
                    .userName(SecurityConfigDefaults.AUTH_ADMIN_USERNAME)
                    .passwd(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        }

        localPravega = emulatorBuilder.build();

        // The uri returned by LocalPravegaEmulator is in the form tcp://localhost:9090 (protocol + domain + port)
        // but for the CLI we need to set the GRPC uri as localhost:9090 (domain + port). Because the protocol
        // is decided based on whether security is enabled or not.

        // Set the CLI properties.
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controllerRestUri", "localhost:" + REST_SERVER_PORT.toString());
        pravegaProperties.setProperty("cli.controllerGrpcUri", "localhost:" + CONTROLLER_PORT.toString());
        pravegaProperties.setProperty("pravegaservice.zkURL", localPravega.getInProcPravegaCluster().getZkUrl());
        pravegaProperties.setProperty("pravegaservice.containerCount", "4");
        pravegaProperties.setProperty("cli.authEnabled", Boolean.toString(AUTH_ENABLED));
        pravegaProperties.setProperty("cli.userName", "admin");
        pravegaProperties.setProperty("cli.password", "1111_aaaa");
        pravegaProperties.setProperty("cli.segmentStore.uri", Inet4Address.getLocalHost().getHostAddress().toString() + ":" + SEGMENT_STORE_PORT.toString());
        STATE.get().getConfigBuilder().include(pravegaProperties);

        localPravega.start();

        // Wait for the server to complete start-up.
        TimeUnit.SECONDS.sleep(20);
    }

    ClientConfig prepareValidClientConfig() {
        return ClientConfig.builder()
                .controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI()))
                .build();
    }

    @After
    public void tearDown() throws Exception {
        if (localPravega != null) {
            localPravega.close();
        }
    }

    @Test
    public void testDescribeStreamCommand() throws Exception {
        String scope = "testScope";
        String testStream = "testStream";
        ClientConfig clientConfig = prepareValidClientConfig();

        // Generate the scope and stream required for testing.
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);

        boolean isScopeCreated = streamManager.createScope(scope);

        // Check if scope created successfully.
        assertTrue("Failed to create scope", isScopeCreated);

        boolean isStreamCreated = streamManager.createStream(scope, testStream, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());

        // Check if stream created successfully.
        Assert.assertTrue("Failed to create the stream ", isStreamCreated);

        String commandResult = executeCommand("controller describe-stream " + scope + " " + testStream, STATE.get());
        Assert.assertTrue(commandResult.contains("stream_config"));
        Assert.assertTrue(commandResult.contains("stream_state"));
        Assert.assertTrue(commandResult.contains("segment_count"));
        Assert.assertTrue(commandResult.contains("is_sealed"));
        Assert.assertTrue(commandResult.contains("active_epoch"));
        Assert.assertTrue(commandResult.contains("truncation_record"));
        Assert.assertTrue(commandResult.contains("scaling_info"));
    }

    static String executeCommand(String inputCommand, AdminCommandState state) throws Exception {
        Parser.Command pc = Parser.parse(inputCommand);
        CommandArgs args = new CommandArgs(pc.getArgs(), state);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        TestingDescribeStreamCommand cmd = new TestingDescribeStreamCommand(args);
        try (PrintStream ps = new PrintStream(baos, true, "UTF-8")) {
            cmd.setOut(ps);
            cmd.execute();
        }
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    private static class TestingDescribeStreamCommand extends ControllerDescribeStreamCommand {

        /**
         * Creates a new instance of the Command class.
         *
         * @param args The arguments for the command.
         */
        public TestingDescribeStreamCommand(CommandArgs args) {
            super(args);
        }

        @Override
        protected SegmentHelper instantiateSegmentHelper(CuratorFramework zkClient) {
            HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                    .hostMonitorEnabled(false)
                    .hostContainerMap(getHostContainerMap(getCLIControllerConfig().getSegmentStoreURI(),
                            getServiceConfig().getContainerCount()))
                    .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                    .containerCount(getServiceConfig().getContainerCount())
                    .build();
            HostControllerStore hostStore = HostStoreFactory.createStore(hostMonitorConfig, StoreClientFactory.createZKStoreClient(zkClient));
            ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(URI.create((getCLIControllerConfig().getControllerGrpcURI())))
                    .validateHostName(false)
                    .credentials(new DefaultCredentials(getCLIControllerConfig().getPassword(),
                            getCLIControllerConfig().getUserName()))
                    .build();
            ConnectionPool pool = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
            return new SegmentHelper(pool, hostStore, pool.getInternalExecutor());
        }

        private Map<Host, Set<Integer>> getHostContainerMap(List<String> uri, int containerCount) {
            Exceptions.checkNotNullOrEmpty(uri, "uri");

            Map<Host, Set<Integer>> hostContainerMap = new HashMap<>();
            uri.forEach(x -> {

                // Get the host and port from the URI
                String host = x.split(":")[0];
                int port = Integer.parseInt(x.split(":")[1]);

                Preconditions.checkNotNull(host, "host");
                Preconditions.checkArgument(port > 0, "port");
                Preconditions.checkArgument(containerCount > 0, "containerCount");

                hostContainerMap.put(new Host(host, port, null), IntStream.range(0, containerCount).boxed().collect(Collectors.toSet()));
            });
            return hostContainerMap;
        }

        @Override
        public void execute() {
            super.execute();
        }
    }
}