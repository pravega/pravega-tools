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

import io.pravega.tools.pravegacli.commands.CommandArgs;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import lombok.Cleanup;
import lombok.val;

import static javax.ws.rs.core.Response.Status.OK;

public class ControllerListScopesCommand extends ControllerCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerListScopesCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(0);

        // Execute listScopes REST API call.
        @Cleanup
        val context = createContext();
        Invocation.Builder builder;
        Response response;
        String controllerURI = "http://localhost:9091";
        String resourceURl = controllerURI + "/v1/scopes";
        WebTarget webTarget = context.client.target(resourceURl);
        builder = webTarget.request();
        response = builder.get();
        assert OK.getStatusCode() == response.getStatus();
        // Print the response sent by the Controller.
        System.out.println(response.readEntity(String.class));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list-scopes", "Lists all the existing scopes in the system.");
    }
}
