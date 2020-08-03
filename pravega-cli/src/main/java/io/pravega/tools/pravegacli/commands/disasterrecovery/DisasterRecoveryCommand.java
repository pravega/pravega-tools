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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableKey;
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
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.NameUtils;
import io.pravega.shared.segment.SegmentToContainerMapper;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.pravega.shared.NameUtils.getMetadataSegmentName;
import static io.pravega.tools.pravegacli.commands.disasterrecovery.StorageListSegmentsCommand.DEFAULT_ROLLING_SIZE;
import static io.pravega.tools.pravegacli.commands.disasterrecovery.StorageListSegmentsCommand.createExecutorService;

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

    ScheduledExecutorService executorService = createExecutorService(100);

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
        }

        // List segments and recover them
        recoverAllSegments(storage, debugStreamSegmentContainerMap, executorService);

        for (int containerId = 0; containerId < getServiceConfig().getContainerCount(); containerId++) {
            // Wait for metadata segment to be flushed to LTS
            String metadataSegmentName = getMetadataSegmentName(containerId);
            waitForSegmentsInStorage(Collections.singleton(metadataSegmentName), debugStreamSegmentContainerMap.get(containerId), storage)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            System.out.println("Long term storage has been update with a new container metadata segment for container Id: " + containerId);

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
            System.out.println("Segment properties = " + sp);
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


    /**
     * Lists all segments from a given long term storage and then re-creates them using their corresponding debug segment
     * container.
     * @param storage                           Long term storage.
     * @param debugStreamSegmentContainers      A hashmap which has debug segment container instances to create segments.
     * @param executorService                   A thread pool for execution.
     * @throws                                  Exception in case of exception during the execution.
     */
    public static void recoverAllSegments(Storage storage, Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainers,
                                          ExecutorService executorService) throws Exception {
        System.out.println("Recovery started for all containers...");
        Map<DebugStreamSegmentContainer, Set<String>> metadataSegmentsByContainer = new HashMap<>();
        for (Map.Entry<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainer : debugStreamSegmentContainers.entrySet()) {
            // Delete container metadata segment and attributes index segment corresponding to the container Id from the long term storage
            deleteContainerMetadataSegments(storage, debugStreamSegmentContainer.getKey());

            ContainerTableExtension ext = debugStreamSegmentContainer.getValue().getExtension(ContainerTableExtension.class);
            AsyncIterator<IteratorItem<TableKey>> it = ext.keyIterator(getMetadataSegmentName(debugStreamSegmentContainer.getKey()),
                    IteratorArgs.builder().fetchTimeout(TIMEOUT).build()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Add all segments present in the container metadata in a set.
            Set<String> metadataSegments = new HashSet<>();
            it.forEachRemaining(k -> metadataSegments.addAll(k.getEntries().stream().map(entry -> entry.getKey().toString())
                    .collect(Collectors.toSet())), executorService).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            metadataSegmentsByContainer.put(debugStreamSegmentContainer.getValue(), metadataSegments);
        }

        SegmentToContainerMapper segToConMapper = new SegmentToContainerMapper(debugStreamSegmentContainers.size());

        Iterator<SegmentProperties> it = storage.listSegments();
        if (it == null) {
            System.out.println("No segments found in the long term storage.");
            return;
        }

        // Iterate through all segments. Create each one of their using their respective debugSegmentContainer instance.
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        while (it.hasNext()) {
            SegmentProperties curr = it.next();
            int containerId = segToConMapper.getContainerId(curr.getName());
            System.out.println("Segment to be recovered = " + curr.getName());
            metadataSegmentsByContainer.get(debugStreamSegmentContainers.get(containerId)).remove(curr.getName());
            futures.add(CompletableFuture.runAsync(new SegmentRecovery(debugStreamSegmentContainers.get(containerId), curr)));
        }
        Futures.allOf(futures).join();

        for (Map.Entry<DebugStreamSegmentContainer, Set<String>> metadataSegmentsSetEntry : metadataSegmentsByContainer.entrySet()) {
            for (String segmentName : metadataSegmentsSetEntry.getValue()) {
                System.out.println("Deleting segment '" + segmentName + "' as it is not in storage");
                metadataSegmentsSetEntry.getKey().deleteStreamSegment(segmentName, TIMEOUT).join();
            }
        }
    }

    /**
     * Creates the given segment with the given DebugStreamSegmentContainer instance.
     */
    public static class SegmentRecovery implements Runnable {
        private final DebugStreamSegmentContainer container;
        private final SegmentProperties storageSegment;

        public SegmentRecovery(DebugStreamSegmentContainer container, SegmentProperties segment) {
            Preconditions.checkNotNull(container);
            Preconditions.checkNotNull(segment);
            this.container = container;
            this.storageSegment = segment;
        }

        @Override
        public void run() {
            long segmentLength = storageSegment.getLength();
            boolean isSealed = storageSegment.isSealed();
            String segmentName = storageSegment.getName();

            System.out.println("Recovering segment with name = " + segmentName + ", length = " + segmentLength + ", sealed status = " + isSealed);
            /*
                1. segment exists in both metadata and storage, re-create it
                2. segment only in metadata, delete
                3. segment only in storage, re-create it
             */
            val streamSegmentInfo = container.getStreamSegmentInfo(storageSegment.getName(), TIMEOUT)
                    .thenAccept(e -> {
                        if (segmentLength != e.getLength() || isSealed != e.isSealed()) {
                            container.deleteStreamSegment(segmentName, TIMEOUT).join();
                            container.registerExistingSegment(segmentName, segmentLength, isSealed).join();
                        }
                    });

            Futures.exceptionallyComposeExpecting(streamSegmentInfo, ex -> Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException,
                    () -> container.registerExistingSegment(segmentName, segmentLength, isSealed)).join();
        }
    }

    /**
     * Deletes container-metadata segment and attribute segment of the container with given container Id.
     * @param storage       Long term storage to delete the segments from.
     * @param containerId   Id of the container for which the segments has to be deleted.
     */
    private static void deleteContainerMetadataSegments(Storage storage, int containerId) {
        String metadataSegmentName = getMetadataSegmentName(containerId);
        deleteSegment(storage, metadataSegmentName);
        String attributeSegmentName = NameUtils.getAttributeSegmentName(metadataSegmentName);
        deleteSegment(storage, attributeSegmentName);
    }

    /**
     * Deletes the segment with given segment name from the given long term storage.
     * @param storage       Long term storage to delete the segment from.
     * @param segmentName   Name of the segment to be deleted.
     */
    private static void deleteSegment(Storage storage, String segmentName) {
        try {
            SegmentHandle segmentHandle = storage.openWrite(segmentName).join();
            storage.delete(segmentHandle, TIMEOUT).join();
        } catch (Exception e) {
            if (Exceptions.unwrap(e) instanceof StreamSegmentNotExistsException) {
                log.info("Segment '{}' doesn't exist.", segmentName);
            } else {
                throw e;
            }
        }
    }
}
