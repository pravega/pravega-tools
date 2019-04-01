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
import javax.ws.rs.core.Response;
import lombok.Cleanup;
import lombok.val;

import static javax.ws.rs.core.Response.Status.OK;

public class ControllerDescribeScopeCommand extends ControllerCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerDescribeScopeCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(1);
        // Describe a the selected scope via REST API.
        @Cleanup
        val context = createContext();
        Response response = executeRESTCall(context, "/v1/scopes/" + getCommandArgs().getArgs().get(0));
        assert OK.getStatusCode() == response.getStatus();
        // Print the response sent by the Controller.
        output(response.readEntity(String.class));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "describe-scope", "Get the details of a given Scope.",
                new ArgDescriptor("scope-name", "Name of the Scope to get details for."));
    }
}
