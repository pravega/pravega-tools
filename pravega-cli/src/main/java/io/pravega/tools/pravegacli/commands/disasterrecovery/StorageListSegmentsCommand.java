/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli.commands.disasterrecovery;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.segment.SegmentToContainerMapper;
import io.pravega.storage.filesystem.FileSystemStorage;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

@Slf4j
public class StorageListSegmentsCommand extends Command {

    protected static final String APPEND_FORMAT = "Segment_%s_Append_%d";
    protected static final long DEFAULT_ROLLING_SIZE = (int) (APPEND_FORMAT.length() * 1.5);
    private SegmentToContainerMapper segToConMapper;
    protected final Logger logger = Logger.getLogger("ListSegmentsLog");
    private static final List<String> HEADER = Arrays.asList("Length", "Sealed Status", "Segment Name");

    public StorageListSegmentsCommand(CommandArgs args) {
        super(args);
        segToConMapper = new SegmentToContainerMapper(getServiceConfig().getContainerCount());
    }

    @Override
    public void execute() throws Exception {
        FileHandler fh;
        fh = new FileHandler("ListSegmentsLog" + System.currentTimeMillis() + ".log");
        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);

        ensureArgCount(1);
        String mountPath = getCommandArgs().getArgs().get(0);
        logging("Mount path of LTS is " + mountPath, Level.INFO);

        String filePath = System.getProperty("user.dir") + File.pathSeparator + "segments";

        if (getArgCount() >= 2) {
            filePath = getCommandArgs().getArgs().get(1);
        }
        logging("Segments' information files are stored in " + filePath, Level.INFO);

        FileSystemStorageConfig fsConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, mountPath)
                .build();
        ScheduledExecutorService scheduledExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(1, "storageProcessor");

        // Get the storage using the config.
        @Cleanup
        Storage storage = new AsyncStorageWrapper(new RollingStorage(new FileSystemStorage(fsConfig), new
                SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), scheduledExecutorService);
        logging(getServiceConfig().getStorageImplementation().toString() + "Storage initialized", Level.FINER);

        int containerCount = segToConMapper.getTotalContainerCount();
        logging("Container Count = " + containerCount, Level.INFO);

        // Create a directory for storing files for each container.
        File dir = new File(filePath);
        if (!dir.exists()) dir.mkdirs();

        // Create a file for each container.
        FileWriter[] csvWriters = new FileWriter[containerCount];
        for (int containerId=0; containerId < containerCount; containerId++) {
            File f = new File(dir, "Container_" + containerId + "_" + System.currentTimeMillis() + ".csv");
            if(f.exists()){
                logging("File already exists " + f.getAbsolutePath(), Level.WARNING);
                if(!f.delete()) {
                    logging("Failed to delete file " + f.getAbsolutePath(), Level.SEVERE);
                    return;
                }
            }
            if(!f.createNewFile()){
                logging("Failed to create " + f.getAbsolutePath(), Level.SEVERE);
                return;
            }
            csvWriters[containerId] = new FileWriter(f.getName());
            logging("Created file " + f.getAbsolutePath(), Level.INFO);
            csvWriters[containerId].append(String.join(",", HEADER));
            csvWriters[containerId].append("\n");
        }

        // Gets total segments listed.
        int segmentCount = 0;

        logging("Writing segments' details onto the files...", Level.INFO);
        Iterator<SegmentProperties> segmentIterator = storage.listSegments();
        while(segmentIterator.hasNext()) {
            segmentCount++;
            SegmentProperties currentSegment = segmentIterator.next();
            int containerId = segToConMapper.getContainerId(currentSegment.getName());
            logging(containerId + "\t" + currentSegment.isSealed() + "\t" + currentSegment.getLength() + "\t" +
                    currentSegment.getName(), Level.FINE);
            csvWriters[containerId].append(currentSegment.isSealed() + "," + currentSegment.getLength() + "," +
                    currentSegment.getName() + "\n");
        }

        logging("Flushing data and closing the files...", Level.FINE);
        for (int containerId=0; containerId < containerCount; containerId++) {
            csvWriters[containerId].flush();
            csvWriters[containerId].close();
        }
        logging("Done listing the segments!", Level.INFO);
    }

    public static CommandDescriptor descriptor() {
        final String component = "storage";
        return new CommandDescriptor(component, "list-segments", "lists segments from tier-2 and displays their name, length, sealed status",
                new ArgDescriptor("root", "mount path"));
    }

    public void logging(String stringToLog, Level level) {
        System.out.println(stringToLog);
        logger.log(level, stringToLog);
    }
}
