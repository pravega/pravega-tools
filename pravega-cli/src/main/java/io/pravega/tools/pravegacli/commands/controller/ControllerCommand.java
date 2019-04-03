/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegacli.commands.controller;

import io.pravega.controller.server.rest.generated.api.JacksonJsonProvider;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import static javax.ws.rs.core.Response.Status.OK;

/**
 * Base for any Controller-related commands.
 */
public abstract class ControllerCommand extends Command {
    static final String COMPONENT = "controller";

    /**
     * Controller commands expect a response from the Controller that we store for further use if necessary.
     */
    @Getter
    protected String response;

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    ControllerCommand(CommandArgs args) {
        super(args);
    }

    /**
     * Creates a context for child classes consisting of a REST client to execute calls against the Controller.
     *
     * @return REST client.
     */
    protected ControllerCommand.Context createContext() {
        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        Client client = ClientBuilder.newClient(clientConfig);
        return new Context(client);
    }

    /**
     * Generic method to execute execute a request against the Controller and get the response.
     *
     * @param context Controller command context.
     * @param requestURI URI to execute the request against.
     * @return Response for the REST call.
     */
    String executeRESTCall(Context context, String requestURI) {
        Invocation.Builder builder;
        String resourceURL = getCLIControllerConfig().getControllerRestURI() + requestURI;
        WebTarget webTarget = context.client.target(resourceURL);
        builder = webTarget.request();
        Response response = builder.get();
        assert OK.getStatusCode() == response.getStatus();
        this.response = response.readEntity(String.class);
        return this.response;
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
