/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli.commands.troubleshoot;

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rest.generated.api.JacksonJsonProvider;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.util.Config;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.AccessLevel;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import org.apache.curator.framework.CuratorFramework;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;

import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

/**
 * Base for any Troubleshoot-related commands.
 */
public abstract class TroubleshootCommandHelper extends Command {
    static final String COMPONENT = "troubleshoot";
    protected StreamMetadataStore store;
    protected ScheduledExecutorService executor ;
    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public TroubleshootCommandHelper(CommandArgs args) {
        super(args);

    }
    /**
     * Creates a context for child classes consisting of a REST client to execute calls against the Troubleshooter.
     *
     * @return REST client.
     */
    protected Context createContext() {

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        Client client = ClientBuilder.newClient(clientConfig);
        // If authorization parameters are configured, set them in the client.
        if (getCLIControllerConfig().getUserName() != null && !getCLIControllerConfig().getUserName().equals("")) {
            HttpAuthenticationFeature auth = HttpAuthenticationFeature.basic(getCLIControllerConfig().getUserName(),
                    getCLIControllerConfig().getPassword());
            client = client.register(auth);
        }
        return new Context(client);
    }

    /**
     * Generic method to execute execute a request against the Controller and get the response.
     *
     * @param context Troubleshoot command context.
     * @param requestURI URI to execute the request against.
     * @return Response for the REST call.
     */
    String executeRESTCall(Context context, String requestURI) {
        Invocation.Builder builder;
        String resourceURL = getCLIControllerConfig().getControllerRestURI() + requestURI;
        WebTarget webTarget = context.client.target(resourceURL);
        builder = webTarget.request();
        Response response = builder.get();
        printResponseInfo(response);
        return response.readEntity(String.class);
    }

    private void printResponseInfo(Response response) {
        if (OK.getStatusCode() == response.getStatus()) {
            output("Successful REST request.");
        } else if (UNAUTHORIZED.getStatusCode() == response.getStatus()) {
            output("Unauthorized REST request. You may need to set the user/password correctly.");
        } else {
            output("The REST request was not successful: " + response.getStatus());
        }
    }

    public SegmentHelper instantiateSegmentHelper(CuratorFramework zkClient) {
        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(true)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .containerCount(getServiceConfig().getContainerCount())
                .build();
        HostControllerStore hostStore = HostStoreFactory.createStore(hostMonitorConfig, StoreClientFactory.createZKStoreClient(zkClient));
        io.pravega.client.ClientConfig clientConfig = io.pravega.client.ClientConfig.builder()
                .controllerURI(URI.create((getCLIControllerConfig().getControllerGrpcURI())))
                .validateHostName(getCLIControllerConfig().isAuthEnabled())
                .credentials(new DefaultCredentials(getCLIControllerConfig().getPassword(), getCLIControllerConfig().getUserName()))
                .build();
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        return new SegmentHelper(connectionFactory, hostStore);
    }

    public StreamMetadataStore createMetadataStore(ScheduledExecutorService executor)
    {
        StreamMetadataStore store;
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        SegmentHelper segmentHelper;
        if (getCLIControllerConfig().getMetadataBackend().equals(CLIControllerConfig.MetadataBackends.ZOOKEEPER.name())) {
            store = StreamStoreFactory.createZKStore(zkClient, executor);
        } else {
            segmentHelper = instantiateSegmentHelper(zkClient);
            GrpcAuthHelper authHelper = GrpcAuthHelper.getDisabledAuthHelper();
            store = StreamStoreFactory.createPravegaTablesStore(segmentHelper, authHelper, zkClient, executor);
            System.out.println("store is here = " +store);
        }
        return store;
    }

    @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
    protected static class Context implements AutoCloseable {
        final Client client;

        @Override
        public void close() {
            this.client.close();
        }
    }
}