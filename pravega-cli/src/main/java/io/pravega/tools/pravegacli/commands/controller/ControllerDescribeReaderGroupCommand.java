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

/**
 * Provides details of a ReaderGroup in a given Scope.
 */
public class ControllerDescribeReaderGroupCommand extends ControllerCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerDescribeReaderGroupCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(2);
        // Describe a the selected scope via REST API.
        @Cleanup
        val context = createContext();
        String scope = getCommandArgs().getArgs().get(0);
        String readerGroup = getCommandArgs().getArgs().get(1);
        Response response = executeRESTCall(context, "/v1/scopes/" + scope + "/readergroups/" + readerGroup);
        assert OK.getStatusCode() == response.getStatus();
        // Print the response sent by the Controller.
        output(response.readEntity(String.class));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "describe-readergroup", "Get the details of a given ReaderGroup in a Scope.",
                new ArgDescriptor("scope-name", "Name of the Scope where the ReaderGroup is stored."),
                new ArgDescriptor("readergroup-id", "Id of the ReaderGroup to describe."));
    }
}
