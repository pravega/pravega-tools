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
import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@Slf4j
public class StorageListSegmentsCommand extends Command {

    protected static final String APPEND_FORMAT = "Segment_%s_Append_%d";
    protected static final long DEFAULT_ROLLING_SIZE = (int) (APPEND_FORMAT.length() * 1.5);
    private SegmentToContainerMapper segToConMapper;

    public StorageListSegmentsCommand(CommandArgs args) {
        super(args);
        segToConMapper = new SegmentToContainerMapper(getServiceConfig().getContainerCount());
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(1);
        String mountPath = getCommandArgs().getArgs().get(0);
        FileSystemStorageConfig fsConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, mountPath)
                .build();
        ScheduledExecutorService scheduledExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(1, "storageProcessor");

        // Get the storage using the config.
        @Cleanup
        Storage storage = new AsyncStorageWrapper(new RollingStorage(new FileSystemStorage(fsConfig), new
                SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), scheduledExecutorService);

        int containerCount = segToConMapper.getTotalContainerCount();

        // Create a directory for storing files for each container.
        String logsDirectory = System.getProperty("user.dir") + File.pathSeparator + "segments";
        File dir = new File(logsDirectory);
        if (!dir.exists()) dir.mkdirs();

        // Create a file for each container.
        FileWriter[] writers = new FileWriter[containerCount];
        for (int containerId=0; containerId < containerCount; containerId++) {
            File f = new File(dir, "Container" + containerId);
            if(f.exists() && !f.delete()){
                System.err.println("Failed to delete "+ f.getAbsolutePath());
                return;
            }
            if(!f.createNewFile()){
                System.err.println("Failed to create "+ f.getAbsolutePath());
                return;
            }
            writers[containerId] = new FileWriter(f.getName());
        }

        System.out.println("Generating container files with the segments they own...");
        Iterator<SegmentProperties> it = storage.listSegments();
        while(it.hasNext()) {
            SegmentProperties currentSegment = it.next();
            int containerId = segToConMapper.getContainerId(currentSegment.getName());
            System.out.println("Segment Name: " + currentSegment.getName() + "\t" + " Sealed status: " + currentSegment.isSealed() +
                    "\t" + " Length: " + currentSegment.getLength());
            writers[containerId].write(currentSegment.getLength() + "\t" + currentSegment.isSealed() + "\t" + currentSegment.getName()
                    + "\n");
        }
        for (int containerId=0; containerId < containerCount; containerId++) {
            writers[containerId].close();
        }
        System.out.println("Done!");
    }

    public static CommandDescriptor descriptor() {
        final String component = "storage";
        return new CommandDescriptor(component, "list-segments", "lists segments from tier-2 and displays their name, length, sealed status",
                new ArgDescriptor("root", "mount path"));
    }
}
