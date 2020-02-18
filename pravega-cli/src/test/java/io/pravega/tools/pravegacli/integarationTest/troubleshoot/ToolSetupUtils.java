package io.pravega.tools.pravegacli.integarationTest.troubleshoot;

import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.netty.impl.ConnectionPoolImpl;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.*;
import io.pravega.common.cluster.Host;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.PravegaTablesStoreHelper;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.util.Config;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.tools.pravegacli.ControllerWrapper;
import io.pravega.tools.pravegacli.commands.AdminCommandState;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.troubleshoot.Fault;
import io.pravega.tools.pravegacli.commands.troubleshoot.Record;
import io.pravega.tools.pravegacli.commands.utils.CLIControllerConfig;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getQualifiedTableName;

/**
 * Utility functions for creating the test setup.
 */
@Slf4j
@NotThreadSafe

public class ToolSetupUtils {
    // The different services.
    @Getter
    private ConnectionFactory connectionFactory = null;
    @Getter
    private Controller controller = null;
    @Getter
    private EventStreamClientFactory clientFactory = null;
    @Getter
    private ControllerWrapper controllerWrapper = null;
    private PravegaConnectionListener server = null;
    @Getter
    private TestingServer zkTestServer = null;
    // Manage the state of the class.
    private final AtomicBoolean started = new AtomicBoolean(false);

    // The test Scope name.
    @Getter
    private final String scope = "scope";
    @Getter
    private final int controllerRPCPort = io.pravega.test.common.TestUtils.getAvailableListenPort();
    @Getter
    private final int controllerRESTPort = TestUtils.getAvailableListenPort();
    @Getter
    private final int servicePort =6000;
    private final ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:" + String.valueOf(controllerRPCPort))).build();
    private String streamName;
    private static final String STREAMS_IN_SCOPE_TABLE_FORMAT = "streamsInScope" + ".#." + "%s";
    private static final String METADATA_TABLE = "metadata" + ".#." + "%s";
    private static final String SCOPES_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, "scopes");
    private final AtomicReference<UUID> scopeidRef=new AtomicReference<>(null);;
    private AtomicReference<String> idRef=new AtomicReference<>(null);
    private  CommandArgs commandArgs;
    private volatile StreamMetadataStore store;
    @Getter
    private SegmentHelper segmentHelper;
    @Getter
    private GrpcAuthHelper authHelper;

    /**
     * Start all pravega related services required for the test deployment.
     *
     * @throws Exception on any errors.
     */
    public void startAllServices() throws Exception {
        startAllServices(null);
    }

    /**
     * Start all pravega related services required for the test deployment.
     *
     * @param numThreads the number of threads for the internal client threadpool.
     * @throws Exception on any errors.
     */
    public void startAllServices(Integer numThreads) throws Exception {
        if (!this.started.compareAndSet(false, true)) {
            log.warn("Services already started, not attempting to start again");
            return;
        }
        this.connectionFactory = new ConnectionFactoryImpl(clientConfig, new ConnectionPoolImpl(clientConfig), numThreads);
        this.controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                connectionFactory.getInternalExecutor());
        this.clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

        // Start zookeeper.
        this.zkTestServer = new TestingServerStarter().start();
        this.zkTestServer.start();
        //log.info("started the zk see =");
        // Start Pravega Service.
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());

        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        this.server = new PravegaConnectionListener(false, servicePort, store, serviceBuilder.createTableStoreService());
        this.server.startListening();
        log.info("Started Pravega Service on port = " + servicePort);

        // Start Controller.
        this.controllerWrapper = new ControllerWrapper(
                this.zkTestServer.getConnectString(), false, true, controllerRPCPort, "localhost", servicePort,
                Config.HOST_STORE_CONTAINER_COUNT, controllerRESTPort);
        this.controllerWrapper.awaitRunning();
        this.controllerWrapper.getController().createScope(scope).get();
        log.info("Initialized Pravega Controller");
    }

    /**
     * Stop the pravega cluster and release all resources.
     *
     * @throws Exception on any errors.
     */
    public void stopAllServices() throws Exception {
        if (!this.started.compareAndSet(true, false)) {
            log.warn("Services not yet started or already stopped, not attempting to stop");
            return;
        }

        this.controllerWrapper.close();
        this.server.close();
        this.zkTestServer.close();
        this.clientFactory.close();
        this.controller.close();
        this.connectionFactory.close();
    }

    /**
     * Create the test stream.
     *
     * @param streamName     Name of the test stream.
     * @param numSegments    Number of segments to be created for this stream.
     *
     * @throws Exception on any errors.
     */
    public void createTestStream(final String streamName, final int numSegments)
            throws Exception {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);
        Preconditions.checkArgument(numSegments > 0);

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName,
                StreamConfiguration.builder()
                        .scalingPolicy(ScalingPolicy.fixed(numSegments))
                        .build());
        log.info("Created stream: " + streamName);
    }

    public void createTestStream_withconfig(final String streamName, final int numSegments, StreamConfiguration configuration)
            throws Exception {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);
        Preconditions.checkArgument(numSegments > 0);
        long start = System.currentTimeMillis();
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName,
                configuration);
        log.info("Created stream: " + streamName);
    }

    /**
     * Create a stream writer for writing Integer events.
     *
     * @param streamName    Name of the test stream.
     *
     * @return Stream writer instance.
     */
    public EventStreamWriter<Integer> getIntegerWriter(final String streamName) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);

        return clientFactory.createEventWriter(streamName, new IntegerSerializer(),
                EventWriterConfig.builder().build());
    }

    /**
     * Create a stream reader for reading Integer events.
     *
     * @param streamName    Name of the test stream.
     *
     * @return Stream reader instance.
     */
    public EventStreamReader<Integer> getIntegerReader(final String streamName) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);

        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        final String readerGroup = "testReaderGroup" + scope + streamName;
        readerGroupManager.createReaderGroup(
                readerGroup,
                ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build());

        final String readerGroupId = UUID.randomUUID().toString();
        return clientFactory.createReader(readerGroupId, readerGroup, new IntegerSerializer(),
                ReaderConfig.builder().build());
    }

    public CompletableFuture<String> getId(String streamname,PravegaTablesStoreHelper storeHelper) {
        streamName=streamname;
        return getStreamsInScopeTableName(storeHelper)
                .thenCompose(streamsInScopeTable -> {
                    return storeHelper.getEntry(streamsInScopeTable, streamname,
                            x -> BitConverter.readUUID(x, 0));
                })
                .thenApply(data -> {
                    idRef.compareAndSet(null, data.getObject().toString());
                    return idRef.get();
                });
    }

    public CompletableFuture<String> getMetadataTable(String streamname, PravegaTablesStoreHelper storeHelper) {
        return getId(streamname,storeHelper).thenApply(this::getMetadataTableName);
    }

    private String getMetadataTableName(String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, this.getScope(),streamName, String.format(METADATA_TABLE, id));
    }

    public CompletableFuture<UUID> getScopeId(PravegaTablesStoreHelper storeHelper) {
        UUID id = scopeidRef.get();
        if (Objects.isNull(id)) {
            return storeHelper.getEntry(SCOPES_TABLE, getScope(), x -> BitConverter.readUUID(x, 0))
                    .thenCompose(entry -> {
                        UUID uuid = entry.getObject();
                        scopeidRef.compareAndSet(null, uuid);
                        return getScopeId(storeHelper);
                    });
        } else {
            return CompletableFuture.completedFuture(id);
        }
    }

    public String faultvalue( Map<Record, Set<Fault>> fault)
    {
        String error = "";
        Iterator iterator =  fault.keySet().iterator();
        while (iterator.hasNext()) {
            Record k = (Record) iterator.next();
            Set<Fault> f = fault.get(k);
            Iterator<Fault> itr = f.iterator();
            error = error + itr.next().getErrorMessage();
        }
        return error;
    }

    public StreamMetadataStore createMetadataStore(ScheduledExecutorService executor, ServiceConfig serviceConfig, CommandArgs cArgs)
    {
        @Cleanup
        CuratorFramework zkClient =createZKClient(serviceConfig);
        commandArgs=cArgs;
        segmentHelper = instantiateSegmentHelper(serviceConfig);
        authHelper = GrpcAuthHelper.getDisabledAuthHelper();
        store = StreamStoreFactory.createPravegaTablesStore(segmentHelper, authHelper, zkClient, executor);
        return store;
    }
    public SegmentHelper instantiateSegmentHelper(ServiceConfig serviceConfig) {
        HashMap<Host, Set<Integer>> hostContainerMap = new HashMap<>();
        hostContainerMap.put(new Host("localhost", 6000, null), IntStream.range(0, 4).boxed().collect(Collectors.toSet()));
        System.out.println(" map = " + hostContainerMap);
        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(true)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .containerCount(serviceConfig.getContainerCount())
                .hostContainerMap(hostContainerMap)
                .build();
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(hostMonitorConfig);
        io.pravega.client.ClientConfig clientConfig = io.pravega.client.ClientConfig.builder()
                .controllerURI(URI.create((getCLIControllerConfig().getControllerGrpcURI())))
                .validateHostName(getCLIControllerConfig().isAuthEnabled())
                .credentials(new DefaultCredentials(getCLIControllerConfig().getPassword(), getCLIControllerConfig().getUserName()))
                .build();
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        return new SegmentHelper(connectionFactory, hostStore);

    }
    public CLIControllerConfig getCLIControllerConfig() {
        return commandArgs.getState().getConfigBuilder().build().getConfig(CLIControllerConfig::builder);
    }

    public CuratorFramework createZKClient(ServiceConfig serviceConfig) {
        CuratorFramework zkClient = CuratorFrameworkFactory
                .builder()
                .connectString(serviceConfig.getZkURL())
                .namespace("pravega/" + serviceConfig.getClusterName())
                .retryPolicy(new ExponentialBackoffRetry(serviceConfig.getZkRetrySleepMs(), serviceConfig.getZkRetryCount()))
                .sessionTimeoutMs(serviceConfig.getZkSessionTimeoutMs())
                .build();
        zkClient.start();
        return zkClient;
    }

    public CompletableFuture<String> getStreamsInScopeTableName(PravegaTablesStoreHelper storeHelper) {
        return getScopeId(storeHelper).thenApply(id ->
                getQualifiedTableName(INTERNAL_SCOPE_NAME, getScope(), String.format(STREAMS_IN_SCOPE_TABLE_FORMAT, id.toString())));
    }
    /**
     * Invoke any command and get the result by using a mock PrintStream object (instead of System.out). The returned
     * String is the output written by the Command that can be check in any test.
     *
     * @param inputCommand Command to execute.
     * @param state        Configuration to execute the command.
     * @return             Output of the command.
     * @throws Exception   If a problem occurs.
     */
    static String executeCommand(String inputCommand, AdminCommandState state) throws Exception {
        Parser.Command pc = Parser.parse(inputCommand);
        CommandArgs args = new CommandArgs(pc.getArgs(), state);
        Command cmd = Command.Factory.get(pc.getComponent(), pc.getName(), args);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true, "UTF-8")) {
            cmd.setOut(ps);
            cmd.execute();
        }
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    public URI getControllerRestUri() {
        return URI.create("http://localhost:" + String.valueOf(controllerRESTPort));
    }

}