/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegastreamstat.service;

import lombok.val;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import io.pravega.tools.pravegastreamstat.logs.LogFormatPrintImpl;
import io.pravega.tools.pravegastreamstat.zookeeper.ZKPrinterImpl;
import io.pravega.tools.pravegastreamstat.storage.hdfs.HDFSStorageFormatPrint;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Tool to get the stream's information.
 */
public class StreamStat {

    // region printers

    private FormatPrinter logFormatPrinter;
    private FormatPrinter zkFormatPrinter;
    private FormatPrinter storageFormatPrinter;

    // endregion

    /**
     * Take the configurations and initialize some data structure
     * @param conf pass the configurations to instance
     */
    private StreamStat(StreamStatConfigure conf) {

        // init each printer with configure
        this.logFormatPrinter = new LogFormatPrintImpl(conf);
        this.zkFormatPrinter = new ZKPrinterImpl(conf);
        this.storageFormatPrinter = new HDFSStorageFormatPrint(conf);
    }

    /**
     * main function reading the command line arguments and set a stream stat instance.
     * @param args command line arguments.
     */
    public static void main(String[] args) {
        // open properties
        Properties pravegaProperties = new Properties();

        try (InputStream input = new FileInputStream(
                System.getProperty("pravega.configurationFile").substring("file:".length()))) {
            pravegaProperties.load(input);
        } catch (Exception e) {
            pravegaProperties.clear();
        }

        // get the command line options

        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
            if (cmd.hasOption('h')) {
                throw new ParseException("Help");
            }
        } catch (ParseException e) {
            System.out.format("%s.%n", e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("StreamStat", options);
            System.exit(1);
        }

        // set configures
        String scopedStreamName = pravegaProperties.getProperty("streamName", Constants.DEFAULT_STREAM_NAME);
        String[] parts = scopedStreamName.split("/");

        if (parts.length != 2 && !cmd.hasOption('i')) {
            ExceptionHandler.INVALID_STREAM_NAME.apply();
        }

        String streamName = parts[1],
                scopeName = parts[0];

        if (cmd.hasOption('i')) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in))) {

                System.out.print("Enter scope name > ");
                scopeName = reader.readLine();
                System.out.print("Enter stream name > ");
                streamName = reader.readLine();

            } catch (IOException e) {
                e.printStackTrace();
                ExceptionHandler.CANNOT_READ_STREAM_NAME.apply();
            }
        }

        int offset = 0;
        if (cmd.hasOption('s')) {
            try {
                offset = Integer.parseInt(cmd.getOptionValue('s'));
            } catch (NumberFormatException e) {
                ExceptionHandler.INVALID_OFFSET_NUMBER.apply();
            }
        }
        final String zkURL = pravegaProperties.getProperty("pravegaservice.zkURL", Constants.DEFAULT_ZK_URL);
        final String hdfsURL = pravegaProperties.getProperty("hdfs.hdfsUrl", Constants.DEFAULT_HDFS_URL);
        final String hdfsRoot = pravegaProperties.getProperty("hdfs.hdfsRoot", Constants.DEFAULT_HDFS_ROOT);
        StreamStatConfigure conf = new StreamStatConfigure();

        conf.setOffset(offset);
        conf.setScope(scopeName);
        conf.setStream(streamName);
        conf.setZkURL(zkURL);
        conf.setHdfsURL(hdfsURL);
        conf.setHdfsRoot(hdfsRoot);

        if (cmd.hasOption('a')) {
            conf.setSimple(false);
        }
        if (cmd.hasOption('d')) {
            conf.setData(true);
        }
        if (cmd.hasOption('c')) {
            conf.setCluster(true);
        }
        if (cmd.hasOption('t')) {
            conf.setTxn(true);
        }
        if (cmd.hasOption('r')) {
            conf.setRecover(true);
        }
        if (cmd.hasOption('e')) {
            conf.setExplicit(true);
        }

        if (cmd.hasOption('l')) {
            conf.setLog(true);
        }

        // service start
        val service =  StreamStat.createStreamStat(conf);
        service.run();

    }

    private static StreamStat createStreamStat(StreamStatConfigure conf) {
        return new StreamStat(conf);
    }

    private void run() {
        print(zkFormatPrinter);
        print(storageFormatPrinter);
        print(logFormatPrinter);
        summarize();
        end();
    }

    private static void print(FormatPrinter formatPrinter) {
        formatPrinter.print();
    }

    private void summarize() {
        ExceptionHandler.summarize();
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("i", "input", false, "Input stream name to get status. " +
                "(You can also set stream name in config file stream=\"[scope]/[stream]\")");
        options.addOption("a", "all", false, "Display all of the container logs.");
        options.addOption("s", "storage", true, "Give the offset to start with.");
        options.addOption("d", "data", false, "Display all the data in the stream. (By default, only print data length)");
        options.addOption("c", "cluster", false, "Display the cluster information.");
        options.addOption("h", "help", false, "Print this help.");
        options.addOption("t", "txn", false, "Print information about all the transactions.");
        options.addOption("r", "recover", false, "Recover mode, if zookeeper's metadata is inconsistent with storage, recreate the metadata.");
        options.addOption("e", "explicit", false, "Wait until get the explicit log," +
                " since the last log might be locked by the pravega cluster");
        options.addOption("l", "log", false, "Display the logs related to given stream in tier-1.");
        return options;

    }

    private void end() {
        PrintHelper.println("ByeBye!");
    }

}