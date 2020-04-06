package io.pravega.tools.pravegacli.commands.disasterrecovery;

import com.google.common.base.Charsets;
import io.pravega.common.concurrent.Services;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.*;
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
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import lombok.val;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;

public class DisasterRecoveryCommand  extends Command implements AutoCloseable{
    private final StreamSegmentContainerFactory containerFactory;
    private String root;
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

    private static final Duration timeout = Duration.ofSeconds(10);
    ScheduledExecutorService executorService = StorageListSegmentsCommand.createExecutorService(10);

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
        //this.storageFactory = new InMemoryStorageFactory();
        val bkConfig = getCommandArgs().getState().getConfigBuilder()
                .include(BookKeeperConfig.builder().with(BookKeeperConfig.ZK_ADDRESS, getServiceConfig().getZkURL()))
                .build().getConfig(BookKeeperConfig::builder);

        val zkClient = createZKClient();
        this.dataLogFactory = new BookKeeperLogFactory(bkConfig, zkClient, executorService);
        //this.dataLogFactory = new InMemoryDurableDataLogFactory(executorService);
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

        //generate segToContainer files
        StorageListSegmentsCommand lsCmd = new StorageListSegmentsCommand(getCommandArgs());
        lsCmd.execute();
        for (int containerId = 0; containerId < getServiceConfig().getContainerCount(); containerId++) {
            DebugStreamSegmentContainer debugStreamSegmentContainer = (DebugStreamSegmentContainer) containerFactory.createDebugStreamSegmentContainer(containerId);
            Services.startAsync(debugStreamSegmentContainer, executorService)
                    .thenRun(new Worker(debugStreamSegmentContainer, containerId))
                    .whenComplete((v, ex) -> Services.stopAsync(debugStreamSegmentContainer, executorService)).join();
        }
    }

    private class Worker implements Runnable {
        private final int containerId;
        private final DebugStreamSegmentContainer container;
        public Worker(DebugStreamSegmentContainer container, int containerId){
            this.container = container;
            this.containerId = containerId;
        }
        @Override
        public void run() {
            System.out.println("=================================================");
            System.out.format("Recovery started for container# %s\n", containerId);
            Scanner s = null;
            try {
                s = new Scanner(new File(String.valueOf(containerId)));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return;
            }
            ContainerTableExtension ext = container.getExtension(ContainerTableExtension.class);
            AsyncIterator<IteratorItem<TableKey>> it = ext.keyIterator(StreamSegmentNameUtils.getMetadataSegmentName(containerId), null, Duration.ofSeconds(10)).join();
            Set<TableKey> segmentsInMD = new HashSet<>();
            it.forEachRemaining(k -> segmentsInMD.addAll(k.getEntries()), executorService);

            while (s.hasNextLine()) {
                String[] fields = s.nextLine().split("\t");
                int len = Integer.parseInt(fields[0]);
                //boolean isSealed = Boolean.parseBoolean(fields[1]);
                String segmentName = fields[2];
                segmentsInMD.remove(TableKey.unversioned(getTableKey(segmentName)));
                /*
                    1. segment exists in both metadata and storage, update SegmentMetadata
                    2. segment only in metadata, delete
                    3. segment only in storage, re-create it
                 */
                container.getStreamSegmentInfo(segmentName, timeout)
                        .thenAccept(e -> {
                            container.deleteStreamSegment(segmentName, timeout).join();
                            container.createStreamSegment(segmentName, len, true).join();
                            System.out.println("Re-created segment :\t" + segmentName);
                        })
                        .exceptionally(e -> {
                                    if (e instanceof StreamSegmentNotExistsException) {
                                        container.createStreamSegment(segmentName, len, true).join();
                                        System.out.println("Created segment :\t" + segmentName);
                                    }
                                return null;
                        });
            }
            for(TableKey k : segmentsInMD){
                String segmentName = new String(k.getKey().array(), Charsets.UTF_8);
                System.out.println("Deleting segment :\t" + segmentName+" as it is not in storage");
                container.deleteStreamSegment(segmentName, timeout).join();
            }
            System.out.format("Recovery done for container# %s\n", containerId);
            System.out.println("=================================================");
        }

    }

    private Map<Class<? extends SegmentContainerExtension>, SegmentContainerExtension> createContainerExtensions(
            SegmentContainer container, ScheduledExecutorService executor) {
        return Collections.singletonMap(ContainerTableExtension.class, new ContainerTableExtensionImpl(container, this.cacheManager, executor));
    }
    private static ArrayView getTableKey(String segmentName) {
        return new ByteArraySegment(segmentName.getBytes(Charsets.UTF_8));
    }
    @Override
    public void close() throws Exception {

    }
    public static CommandDescriptor descriptor() {
        final String component = "dr";
        return new CommandDescriptor(component, "recover", "reconcile segments from container",
                new ArgDescriptor("root", "root of the file system"));
    }
}
