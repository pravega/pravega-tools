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

import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.controller.server.rpc.auth.StrongPasswordProcessor;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.List;

public class PasswordFileCreatorCommand extends Command {
    static final String COMPONENT = "admin";
    public PasswordFileCreatorCommand(CommandArgs args){super(args);}
    private String toWrite;

    public String getToWrite(){
        return this.toWrite;
    }

    @Override
    public void execute() throws InvalidKeySpecException, NoSuchAlgorithmException, IOException {
        try {
            ensureArgCount(2);
            String targetFileName = getTargetFilename(getCommandArgs().getArgs());
            String userDetails = getUserDetails(getCommandArgs().getArgs());
            createPassword(userDetails);
            writeToFile(targetFileName, toWrite);
        }
        catch (Exception e){
            System.err.println(e.getMessage());
        }
    }

    public String getTargetFilename(List<String> userInput) {
        return userInput.get(0);
    }

    public String getUserDetails(List<String> userInput){
        String userDetails = userInput.get(1);
        if((userDetails.split(":")).length == 3){
            return userDetails;
        }
        else throw new IllegalArgumentException("The user detail entered is not of the format uname:pwd:acl");
    }

    public void createPassword(String userDetails) throws NoSuchAlgorithmException, InvalidKeySpecException {
        String[] lists = parseUserDetails(userDetails);
        toWrite = generatePassword(lists);
    }

    private String[] parseUserDetails(String userDetails){
        return userDetails.split(":");
    }

    private String generatePassword(String[] lists) throws NoSuchAlgorithmException, InvalidKeySpecException{
        StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();
        return lists[0] + ":" + passwordEncryptor.encryptPassword(lists[1]) + ":" + lists[2] + ";";
    }

    private void writeToFile(String targetFileName, String toWrite) throws IOException {
        try (FileWriter writer = new FileWriter(targetFileName))
        {
            writer.write(toWrite + "\n");
            writer.flush();
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "create-password-file", "Generates file with encrypted password using filename and user:password:acl given as argument.", new ArgDescriptor("filename", "Name of the file generated by the command"), new ArgDescriptor("user:passwword:acl", "Input according to which encrypted password is generated"));
    }
}

