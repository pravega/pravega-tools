/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli;

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

public class AdminCommandsTest {

   // @Rule
    //public TemporaryFolder folder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testPasswordFileCreatorCommand() throws Exception {
        try {
            //String commandResult = TestUtils.executeCommand("admin password-file-creator", STATE.get());
            //System.err.println(commandResult);
            AdminCommandState STATE = new AdminCommandState();
            List<String> arguments = new ArrayList<String>();
            arguments.add("TestFile");
            arguments.add("testing:testing:testing");
            CommandArgs args = new CommandArgs(arguments,STATE);
            PasswordFileCreatorCommand testObj = new PasswordFileCreatorCommand(args);
            //File TestFile = folder.newFile("TestFile.txt");
            testObj.execute();
            Assert.assertTrue(testObj.toWrite.split(":")[0].equals("testing"));
            Assert.assertTrue(testObj.toWrite.split(":")[2].equals("testing;"));
            //Assert.assertTrue(Files.deleteIfExists("TestFile"));
        }
        catch(InvalidKeySpecException e)
        {
            Assert.assertTrue((e.getMessage()).contains("InvalidKeySpec"));
        }
        catch(NoSuchAlgorithmException e)
        {
            Assert.assertTrue((e.getMessage()).contains("NoSuchAlgorithm"));
        }
        catch(IOException e)
        {
            Assert.assertTrue((e.getMessage()).contains("IO"));
        }
    }

}
