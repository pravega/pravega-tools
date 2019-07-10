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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.common.base.Strings;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.config.ConfigListCommand;
import io.pravega.tools.pravegacli.commands.utils.ConfigUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.val;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the Pravega CLI.
 */
public final class AdminRunner {
    private static final String CMD_HELP = "help";
    private static final String CMD_EXIT = "exit";

    /**
     * Main entry point for the Admin Tools Runner.
     * <p>
     * To speed up setup, create a config.properties file and put the following properties (at a minimum):
     * <p>
     * pravegaservice.containerCount={number of containers}
     * pravegaservice.zkURL={host:port for ZooKeeper}
     * cli.controllerRestUri={host:port for a Controller REST API endpoint}
     * bookkeeper.bkLedgerPath={path in ZooKeeper where BookKeeper stores Ledger metadata}
     * <p>
     * This program can be executed in two modes. First, the "interactive mode", in which you may want to point to a
     * config file that contains the previous mandatory configuration parameters:
     * -Dpravega.configurationFile=config.properties
     *
     * If you don't want to use a config file, you still can load configuration properties dynamically using the command
     * "config set property=value".
     *
     * Second, this program can be executed in "batch mode" to execute a single command. To this end, you need to pass
     * as program argument the command to execute and as properties the configuration parameters (-D flag):
     * ./pravega-cli controller list-scopes -Dpravegaservice.containerCount={value} -Dpravegaservice.zkURL={value} ...
     *
     * @param args Arguments.
     * @throws Exception If one occurred.
     */
    public static void main(String[] args) throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.ERROR);

        System.out.println("Pravega CLI.\n");
        @Cleanup
        AdminCommandState state = new AdminCommandState();
        ConfigUtils.loadProperties(state);

        // Output loaded config.
        System.out.println("Initial configuration:");
        val initialConfigCmd = new ConfigListCommand(new CommandArgs(Collections.emptyList(), state));
        initialConfigCmd.execute();

        if (args == null || args.length == 0) {
            interactiveMode(state);
        } else {
            String commandLine = Arrays.stream(args).collect(Collectors.joining(" ", "", ""));
            processCommand(commandLine, state);
        }
        System.exit(0);
    }

    private static void interactiveMode(AdminCommandState state) {
        // Continuously accept new commands as long as the user entered one.
        System.out.println(String.format("%nType \"%s\" for list of commands, or \"%s\" to exit.", CMD_HELP, CMD_EXIT));
        @Cleanup
        Scanner input = new Scanner(System.in);
        while (true) {
            System.out.print(System.lineSeparator() + "> ");
            String line = input.nextLine();
            processCommand(line, state);
        }
    }

    private static void processCommand(String line, AdminCommandState state) {
        if (Strings.isNullOrEmpty(line.trim())) {
            return;
        }

        Parser.Command pc = Parser.parse(line);
        switch (pc.getComponent()) {
            case CMD_HELP:
                printHelp(null);
                break;
            case CMD_EXIT:
                System.exit(0);
                break;
            default:
                execCommand(pc, state);
                break;
        }
    }

    private static void execCommand(Parser.Command pc, AdminCommandState state) {
        CommandArgs cmdArgs = new CommandArgs(pc.getArgs(), state);
        try {
            Command cmd = Command.Factory.get(pc.getComponent(), pc.getName(), cmdArgs);
            if (cmd == null) {
                // No command was found.
                printHelp(pc);
            } else {
                cmd.execute();
            }
        } catch (IllegalArgumentException ex) {
            // We found a command, but had the wrong arguments to it.
            System.out.println("Bad command syntax: " + ex.getMessage());
            printCommandDetails(pc);
        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
    }

    private static void printCommandSummary(Command.CommandDescriptor d) {
        System.out.println(String.format("\t%s %s %s: %s",
                d.getComponent(),
                d.getName(),
                Arrays.stream(d.getArgs()).map(AdminRunner::formatArgName).collect(Collectors.joining(" ")),
                d.getDescription()));
    }

    private static void printCommandDetails(Parser.Command command) {
        Command.CommandDescriptor d = Command.Factory.getDescriptor(command.getComponent(), command.getName());
        if (d == null) {
            printHelp(command);
            return;
        }

        printCommandSummary(d);
        for (Command.ArgDescriptor ad : d.getArgs()) {
            System.out.println(String.format("\t\t%s: %s", formatArgName(ad), ad.getDescription()));
        }
    }

    private static void printHelp(Parser.Command command) {
        Collection<Command.CommandDescriptor> commands;
        if (command == null) {
            // All commands.
            commands = Command.Factory.getDescriptors();
            System.out.println("All available commands:");
        } else {
            // Commands specific to a component.
            commands = Command.Factory.getDescriptors(command.getComponent());
            if (commands.isEmpty()) {
                System.out.println(String.format("No commands are available for component '%s'.", command.getComponent()));
            } else {
                System.out.println(String.format("All commands for component '%s':", command.getComponent()));
            }
        }

        Map<String, Integer> troubleshootNumbering = new HashMap<>();
        troubleshootNumbering.put("print-metadata", 1);
        troubleshootNumbering.put("diagnosis", 2);
        troubleshootNumbering.put("general-check", 3);
        troubleshootNumbering.put("scale-check", 4);
        troubleshootNumbering.put("committing_txn-check", 5);
        troubleshootNumbering.put("truncate-check", 6);
        troubleshootNumbering.put("update-check", 7);

        commands.stream()
                .sorted(Comparator.comparing(Command.CommandDescriptor::getComponent))
                .sorted((c1, c2) -> {
                    if (c1.getComponent().equals("troubleshoot") && c2.getComponent().equals("troubleshoot")) {
                        return troubleshootNumbering.get(c1.getName()).compareTo(troubleshootNumbering.get(c2.getName()));
                    }
                    return c1.getComponent().compareTo(c2.getComponent());
                })
                .forEach(AdminRunner::printCommandSummary);
    }

    private static String formatArgName(Command.ArgDescriptor ad) {
        return String.format("<%s>", ad.getName());
    }
}
