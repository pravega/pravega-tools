package io.pravega.tools.pravegacli.commands.disasterrecovery;

import com.google.common.base.Charsets;
import io.pravega.common.concurrent.Services;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.server.*;
import io.pravega.segmentstore.server.attributes.AttributeIndexConfig;
import io.pravega.segmentstore.server.attributes.AttributeIndexFactory;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndexFactoryImpl;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainer;
import io.pravega.segmentstore.server.containers.MetadataStore;
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
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import lombok.val;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

public class DisasterRecoveryCommand  extends Command implements AutoCloseable{
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
    ScheduledExecutorService executorService = getCommandArgs().getState().getExecutor();

    public DisasterRecoveryCommand(CommandArgs args) {
        super(args);
        val config = getCommandArgs().getState().getConfigBuilder().build().getConfig(ContainerConfig::builder);
        //TODO: which storageFactory to instantiate?
        this.storageFactory = new InMemoryStorageFactory(executorService);
        /*
        val bkConfig = getCommandArgs().getState().getConfigBuilder()
                .include(BookKeeperConfig.builder().with(BookKeeperConfig.ZK_ADDRESS, getServiceConfig().getZkURL()))
                .build().getConfig(BookKeeperConfig::builder);

        val zkClient = createZKClient();
        this.dataLogFactory = new BookKeeperLogFactory(bkConfig, zkClient, executorService);
        */
        this.dataLogFactory = new InMemoryDurableDataLogFactory(executorService);
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


    private static final String BACKUP_PREFIX = "backup";
    public void execute(){
        for (int containerId = 0; containerId < getServiceConfig().getContainerCount(); containerId++) {
            DebugStreamSegmentContainer debugStreamSegmentContainer = (DebugStreamSegmentContainer) containerFactory.createDebugStreamSegmentContainer(containerId);
            Services.startAsync(debugStreamSegmentContainer, executorService).thenRun(new Worker(debugStreamSegmentContainer, containerId));
        }
    }
    private static class Worker implements Runnable {
        private final int containerId;
        private final DebugStreamSegmentContainer container;
        public Worker(DebugStreamSegmentContainer container, int containerId){
            this.container = container;
            this.containerId = containerId;
        }
        @Override
        public void run() {
            container.awaitRunning();
            System.out.format("Recovery started for container# %s\n", containerId);
            Scanner s = null;
            try {
                s = new Scanner(new File(String.valueOf(containerId)));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            List<ArrayView> segments = new ArrayList<>();
            while (s.hasNextLine()) {
                String[] fields = s.nextLine().split("\t");
                System.out.println("Creating segment for :\t" + Arrays.toString(fields));
                int len = Integer.parseInt(fields[0]);
                boolean isSealed = Boolean.parseBoolean(fields[1]);
                String segmentName = fields[2];
                //TODO: verify the return status
                try {
                    container.createStreamSegment(segmentName, len, isSealed).get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                System.out.format("Segment created for %s\n", segmentName);
                segments.add(getTableKey(segmentName));
            }
            String mFileName = StreamSegmentNameUtils.getMetadataSegmentName(containerId);
            File metadataFile = new File(mFileName);
            boolean isRenamed = metadataFile.renameTo(new File(getBackupMetadataFileName(mFileName)));
            if (!isRenamed) {
                System.out.println("Rename failed for " + mFileName);
                return;
            }
            System.out.format("Renamed %s to %s\n", mFileName, getBackupMetadataFileName(mFileName));
            ContainerTableExtension ext = container.getExtension(ContainerTableExtension.class);
            List<TableEntry> entries = null;
            try {
                entries = ext.get(getBackupMetadataFileName(mFileName), segments, Duration.ofSeconds(10)).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            for (int i = 0; i < entries.size(); i++) {
                TableEntry entry = entries.get(i);
                String segmentName = new String(segments.get(i).array(), Charsets.UTF_8);
                System.out.format("Adjusting the metadata for segment %s in container# %s\n", segmentName, containerId);
                SegmentProperties segProp = MetadataStore.SegmentInfo.deserialize(entry.getValue()).getProperties();
                if (segProp.isSealed())
                    container.sealStreamSegment(segmentName, Duration.ofSeconds(10));
                List<AttributeUpdate> updates = new ArrayList<>();
                for (Map.Entry<UUID, Long> e : segProp.getAttributes().entrySet())
                    updates.add(new AttributeUpdate(e.getKey(), AttributeUpdateType.Replace, e.getValue()));
                container.updateAttributes(segmentName, updates, Duration.ofSeconds(10));
            }
            System.out.format("Recovery done for container# %s\n", containerId);
        }
    }
    private static String getBackupMetadataFileName(String metadataFileName){
        return BACKUP_PREFIX+metadataFileName;
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
        return new CommandDescriptor(component, "recover", "reconcile segments from container");
    }
}
