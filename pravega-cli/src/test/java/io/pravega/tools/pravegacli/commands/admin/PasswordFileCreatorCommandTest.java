/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli.commands.admin;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import io.pravega.test.integration.utils.SetupUtils;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import org.junit.Before;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import java.util.Properties;
import io.pravega.tools.pravegacli.commands.admin.PasswordFileCreatorCommand;
import java.util.List;
import java.util.ArrayList;
import org.junit.rules.TemporaryFolder;
import org.junit.Rule;
import java.io.File;
import java.nio.file.*;

public class PasswordFileCreatorCommandTest {

    @Test
    public void testcreatePassword() throws Exception {
        try {
            AdminCommandState state = new AdminCommandState();
            List<String> arguments = new ArrayList<String>();
            String s1 = "TestFile";
            String s2 = "testing:testing:testing";
            arguments.add(s1);
            arguments.add(s2);
            CommandArgs args = new CommandArgs(arguments, state);
            PasswordFileCreatorCommand testObj = new PasswordFileCreatorCommand(args);
            testObj.createPassword("testing:testing:testing");
            Assert.assertTrue(testObj.getToWrite().split(":")[0].equals("testing"));
            Assert.assertTrue(testObj.getToWrite().split(":")[2].equals("testing;"));
        } catch(Exception e){
            System.err.println(e.getMessage());
        }
    }

    @Test
    public void testgetTargetFilename() throws Exception {
        AdminCommandState STATE = new AdminCommandState();
        List<String> arguments = new ArrayList<String>();
        String s1 = "TestFile";
        String s2 = "testing:testing:testing";
        arguments.add(s1);
        arguments.add(s2);
        CommandArgs args = new CommandArgs(arguments, STATE);
        PasswordFileCreatorCommand testObj = new PasswordFileCreatorCommand(args);
        Assert.assertTrue(testObj.getTargetFilename(arguments).equals("TestFile"));
    }

    @Test
    public void testgetUserDetails() throws Exception {
        AdminCommandState STATE = new AdminCommandState();
        List<String> arguments = new ArrayList<String>();
        String s1 = "TestFile";
        String s2 = "testing:testing:testing";
        arguments.add(s1);
        arguments.add(s2);
        CommandArgs args = new CommandArgs(arguments, STATE);
        PasswordFileCreatorCommand testObj = new PasswordFileCreatorCommand(args);
        Assert.assertTrue(testObj.getUserDetails(arguments).equals("testing:testing:testing"));
    }
}