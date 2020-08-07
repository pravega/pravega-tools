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

import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.attributes.AttributeIndexConfig;
import io.pravega.segmentstore.server.attributes.AttributeIndexFactory;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndexFactoryImpl;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainer;
import io.pravega.segmentstore.server.containers.SegmentsRecovery;
import io.pravega.segmentstore.server.containers.StreamSegmentContainerFactory;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.server.reading.ContainerReadIndexFactory;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.server.tables.ContainerTableExtensionImpl;
import io.pravega.segmentstore.server.writer.StorageWriterFactory;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.NameUtils;
import io.pravega.storage.filesystem.FileSystemStorage;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.pravega.shared.NameUtils.getMetadataSegmentName;
import static io.pravega.tools.pravegacli.commands.disasterrecovery.StorageListSegmentsCommand.DEFAULT_ROLLING_SIZE;

@Slf4j
public class DisasterRecoveryCommand extends Command implements AutoCloseable {
    protected static final Duration TIMEOUT = Duration.ofMillis(100 * 1000);
    private String root;
    private final StreamSegmentContainerFactory containerFactory;
    private final StorageFactory storageFactory;
    private final DurableDataLogFactory dataLogFactory;
    private final OperationLogFactory operationLogFactory;
    private final ReadIndexFactory readIndexFactory;
    private final AttributeIndexFactory attributeIndexFactory;
    private final WriterFactory writerFactory;
    private final CacheStorage cacheStorage;
    private final CacheManager cacheManager;
    private static final DurableLogConfig DEFAULT_DURABLE_LOG_CONFIG = DurableLogConfig
            .builder()
            .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10)
            .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
            .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10 * 1024 * 1024L)
            .with(DurableLogConfig.START_RETRY_DELAY_MILLIS, 20)
            .build();
    private static final ReadIndexConfig DEFAULT_READ_INDEX_CONFIG = ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024).build();

    private static final AttributeIndexConfig DEFAULT_ATTRIBUTE_INDEX_CONFIG = AttributeIndexConfig
            .builder()
            .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, 2 * 1024)
            .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 1000)
            .build();

    private static final WriterConfig DEFAULT_WRITER_CONFIG = WriterConfig
            .builder()
            .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1)
            .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 25L)
            .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
            .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L)
            .build();

    ScheduledExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(100, "recoveryProcessor");

    public DisasterRecoveryCommand(CommandArgs args) {
        super(args);
        ensureArgCount(1);
        root = getCommandArgs().getArgs().get(0);
        if(!root.endsWith("/"))
            root += "/";

        val config = getCommandArgs().getState().getConfigBuilder().build().getConfig(ContainerConfig::builder);
        //TODO: which storageFactory to instantiate?
        FileSystemStorageConfig fsConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, getCommandArgs().getArgs().get(0))
                .build();
        this.storageFactory = new FileSystemStorageFactory(fsConfig, executorService);
        val bkConfig = getCommandArgs().getState().getConfigBuilder()
                .include(BookKeeperConfig.builder().with(BookKeeperConfig.ZK_ADDRESS, getServiceConfig().getZkURL()))
                .build().getConfig(BookKeeperConfig::builder);

        val zkClient = createZKClient();
        this.dataLogFactory = new BookKeeperLogFactory(bkConfig, zkClient, executorService);
        try {
            this.dataLogFactory.initialize();
        } catch (DurableDataLogException e) {
            e.printStackTrace();
        }
        this.operationLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, dataLogFactory, executorService);
        this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
        this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, executorService);
        this.readIndexFactory = new ContainerReadIndexFactory(DEFAULT_READ_INDEX_CONFIG, this.cacheManager, executorService);
        this.attributeIndexFactory = new ContainerAttributeIndexFactoryImpl(DEFAULT_ATTRIBUTE_INDEX_CONFIG, this.cacheManager, executorService);
        this.writerFactory = new StorageWriterFactory(DEFAULT_WRITER_CONFIG, executorService);
        this.containerFactory = new StreamSegmentContainerFactory(config, this.operationLogFactory,
                this.readIndexFactory, this.attributeIndexFactory, this.writerFactory, this.storageFactory,
                this::createContainerExtensions, executorService);
    }

    public void execute() throws Exception {
        FileSystemStorageConfig fsConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, root)
                .build();
        @Cleanup
        Storage storage = new AsyncStorageWrapper(new RollingStorage(new FileSystemStorage(fsConfig), new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)),
                executorService);

        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = new HashMap<>();

        System.out.println("Starting all debug segment containers...");
        for (int containerId = 0; containerId < getServiceConfig().getContainerCount(); containerId++) {
            // Start a debug segment container with given id and recover all segments belonging to that container
            DebugStreamSegmentContainer debugStreamSegmentContainer = (DebugStreamSegmentContainer)
                    containerFactory.createDebugStreamSegmentContainer(containerId);
            Services.startAsync(debugStreamSegmentContainer, executorService).join();
            debugStreamSegmentContainerMap.put(containerId, debugStreamSegmentContainer);
            System.out.println("Debug Segment container " + containerId + " started.");

            // Delete container metadata segment and attributes index segment corresponding to the container Id from the long term storage
            SegmentsRecovery.deleteContainerMetadataSegments(storage, containerId);
            System.out.println("Container metadata segment and attributes index segment deleted for container Id = " + containerId);
        }

        // List segments and recover them
        SegmentsRecovery.recoverAllSegments(storage, debugStreamSegmentContainerMap, executorService);

        for (int containerId = 0; containerId < getServiceConfig().getContainerCount(); containerId++) {
            // Wait for metadata segment to be flushed to LTS
            String metadataSegmentName = getMetadataSegmentName(containerId);
            waitForSegmentsInStorage(Collections.singleton(metadataSegmentName), debugStreamSegmentContainerMap.get(containerId), storage)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Stop the debug segment container
            Services.stopAsync(debugStreamSegmentContainerMap.get(containerId), executorService).join();
            debugStreamSegmentContainerMap.get(containerId).close();
            System.out.println("Segments have been recovered for container Id: " + containerId);
        }
    }

    private Map<Class<? extends SegmentContainerExtension>, SegmentContainerExtension> createContainerExtensions(
            SegmentContainer container, ScheduledExecutorService executor) {
        return Collections.singletonMap(ContainerTableExtension.class, new ContainerTableExtensionImpl(container, this.cacheManager, executor));
    }

    public static CommandDescriptor descriptor() {
        final String component = "dr";
        return new CommandDescriptor(component, "recover", "reconcile segments from container",
                new ArgDescriptor("root", "root of the file system"));
    }

    @Override
    public void close() {
        this.cacheManager.close();
        this.cacheStorage.close();
        this.readIndexFactory.close();
        this.dataLogFactory.close();
    }

    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, DebugStreamSegmentContainer container,
                                                             Storage storage) {
        ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
        for (String segmentName : segmentNames) {
            SegmentProperties sp = container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            segmentsCompletion.add(waitForSegmentInStorage(sp, storage));
        }

        return Futures.allOf(segmentsCompletion);
    }

    private CompletableFuture<Void> waitForSegmentInStorage(SegmentProperties sp, Storage storage) {
        if (sp.getLength() == 0) {
            // Empty segments may or may not exist in Storage, so don't bother complicating ourselves with this.
            return CompletableFuture.completedFuture(null);
        }

        // We want to make sure that both the main segment and its attribute segment have been sync-ed to Storage. In case
        // of the attribute segment, the only thing we can easily do is verify that it has been sealed when the main segment
        // it is associated with has also been sealed.
        String attributeSegmentName = NameUtils.getAttributeSegmentName(sp.getName());
        TimeoutTimer timer = new TimeoutTimer(TIMEOUT);
        AtomicBoolean tryAgain = new AtomicBoolean(true);
        return Futures.loop(
                tryAgain::get,
                () -> {
                    val segInfo = getStorageSegmentInfo(sp.getName(), timer, storage);
                    val attrInfo = getStorageSegmentInfo(attributeSegmentName, timer, storage);
                    return CompletableFuture.allOf(segInfo, attrInfo)
                            .thenCompose(v -> {
                                SegmentProperties storageProps = segInfo.join();
                                SegmentProperties attrProps = attrInfo.join();
                                if (sp.isSealed()) {
                                    tryAgain.set(!storageProps.isSealed() || !(attrProps.isSealed() || attrProps.isDeleted()));
                                } else {
                                    tryAgain.set(sp.getLength() != storageProps.getLength());
                                }

                                if (tryAgain.get() && !timer.hasRemaining()) {
                                    return Futures.<Void>failedFuture(new TimeoutException(
                                            String.format("Segment %s did not complete in Storage in the allotted time.", sp.getName())));
                                } else {
                                    return Futures.delayedFuture(Duration.ofMillis(100), executorService);
                                }
                            });
                },
                executorService);
    }

    private CompletableFuture<SegmentProperties> getStorageSegmentInfo(String segmentName, TimeoutTimer timer, Storage storage) {
        return Futures
                .exceptionallyExpecting(storage.getStreamSegmentInfo(segmentName, timer.getRemaining()),
                        ex -> ex instanceof StreamSegmentNotExistsException,
                        StreamSegmentInformation.builder().name(segmentName).deleted(true).build());
    }
}
