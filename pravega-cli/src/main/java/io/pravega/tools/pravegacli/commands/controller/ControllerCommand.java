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
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

public abstract class ControllerCommand extends Command {
    static final String COMPONENT = "controller";

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    ControllerCommand(CommandArgs args) {
        super(args);
    }

    protected ControllerCommand.Context createContext() {
        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        Client client = ClientBuilder.newClient(clientConfig);
        return new Context(client);
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
