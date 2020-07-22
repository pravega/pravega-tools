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

import com.google.common.collect.ImmutableMap;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import lombok.Cleanup;
import lombok.val;

/**
 * Create a new scope.
 */
public class ControllerCreateScopeCommand extends ControllerCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerCreateScopeCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(1);
        // Describe a the selected scope via REST API.
        @Cleanup
        val context = createContext();
        // Print the response sent by the Controller.
        String payload = toJson(ImmutableMap.of("scopeName", getCommandArgs().getArgs().get(0)));
        prettyJSONOutput(executeRESTCall(context, "POST", "/v1/scopes", payload));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "create-scope", "Create a new Scope.",
                new ArgDescriptor("scope-name", "Name of the Scope to create."));
    }
}
