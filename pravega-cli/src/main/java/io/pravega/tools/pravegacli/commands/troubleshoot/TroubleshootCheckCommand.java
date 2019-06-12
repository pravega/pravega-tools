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

import io.pravega.tools.pravegacli.commands.CommandArgs;

public class TroubleshootCheckCommand extends TroubleshootCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public TroubleshootCheckCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(2);

        EpochRecordHistoryTimeSeriesCheck ephtChecker = new EpochRecordHistoryTimeSeriesCheck(getCommandArgs());
        ephtChecker.check();
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "diagnosis", "Just testing the command",
                new ArgDescriptor("scope-name", "Name of the scope"),
                new ArgDescriptor("stream-name", "Name of the stream"));
    }
}
