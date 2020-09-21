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

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.local.LocalPravegaEmulator;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class SecureControllerCommandsTest {

    // Security related flags and instantiate local pravega server.
    private static final boolean REST_ENABLED = true;
    private static final boolean AUTH_ENABLED = true;
    private static final boolean TLS_ENABLED = true;
    private static final Integer REST_SERVER_PORT = 9091;
    private static LocalPravegaEmulator localPravega;
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    @Before
    public void setUp() throws Exception {

        // Create the secure pravega server to test commands against.
        LocalPravegaEmulator.LocalPravegaEmulatorBuilder emulatorBuilder = LocalPravegaEmulator.builder()
                .controllerPort(io.pravega.test.common.TestUtils.getAvailableListenPort())
                .segmentStorePort(io.pravega.test.common.TestUtils.getAvailableListenPort())
                .zkPort(io.pravega.test.common.TestUtils.getAvailableListenPort())
                .restServerPort(io.pravega.test.common.TestUtils.getAvailableListenPort())
                .enableRestServer(REST_ENABLED)
                .enableAuth(AUTH_ENABLED)
                .enableTls(TLS_ENABLED)
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
        if (TLS_ENABLED) {
            emulatorBuilder.certFile("src/test/resources/server-cert.crt")
                    .keyFile("src/test/resources/server-key.key")
                    .jksKeyFile("src/test/resources/server.keystore.jks")
                    .jksTrustFile("src/test/resources/client.truststore.jks")
                    .keyPasswordFile("src/test/resources/server.keystore.jks.passwd");
        }

        localPravega = emulatorBuilder.build();

        // Set the CLI properties.
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controllerRestUri", "localhost:" + REST_SERVER_PORT.toString());
        pravegaProperties.setProperty("pravegaservice.zkURL", localPravega.getInProcPravegaCluster().getZkUrl());
        pravegaProperties.setProperty("pravegaservice.containerCount", "4");
        pravegaProperties.setProperty("cli.authEnabled", "true");
        pravegaProperties.setProperty("cli.tlsEnabled", "true");
        pravegaProperties.setProperty("cli.userName", "admin");
        pravegaProperties.setProperty("cli.password", "1111_aaaa");
        pravegaProperties.setProperty("cli.security.tls.trustStore.location", "src/test/resources/client.truststore.jks");
        STATE.get().getConfigBuilder().include(pravegaProperties);

        localPravega.start();

        // Wait for the server to complete start-up.
        TimeUnit.SECONDS.sleep(20);
    }

    ClientConfig prepareValidClientConfig() {
        return ClientConfig.builder()
                .controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI()))

                // TLS-related
                .trustStore("src/test/resources/ca-cert.crt")
                .validateHostName(false)

                // Auth-related
                .credentials(new DefaultCredentials(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD,
                        SecurityConfigDefaults.AUTH_ADMIN_USERNAME))
                .build();
    }

    @After
    public void tearDown() throws Exception {
        if (localPravega != null) {
            localPravega.close();
        }
    }

    @Test
    public void testListScopesCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("controller list-scopes", STATE.get());
        Assert.assertTrue(commandResult.contains("_system"));
    }

    @Test
    public void testListStreamsCommand() throws Exception {
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

        String commandResult = TestUtils.executeCommand("controller list-streams " + scope, STATE.get());
        Assert.assertTrue(commandResult.contains(testStream));
    }

    @Test
    public void testListReaderGroupsCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("controller list-readergroups _system", STATE.get());
        Assert.assertTrue(commandResult.contains("commitStreamReaders"));
    }

    @Test
    public void testDescribeScopeCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("controller describe-scope _system", STATE.get());
        Assert.assertTrue(commandResult.contains("_system"));
    }
}
