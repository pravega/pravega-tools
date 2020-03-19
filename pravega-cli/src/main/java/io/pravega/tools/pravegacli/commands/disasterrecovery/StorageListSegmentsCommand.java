package io.pravega.tools.pravegacli.commands.disasterrecovery;

import io.pravega.shared.segment.SegmentToContainerMapper;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.storage.filesystem.FileSystemStorage;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.tools.pravegacli.commands.controller.ControllerCommand;
import lombok.AccessLevel;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;


public class StorageListSegmentsCommand extends Command {


    protected static final String APPEND_FORMAT = "Segment_%s_Append_%d";
    protected static final long DEFAULT_ROLLING_SIZE = (int) (APPEND_FORMAT.length() * 1.5);
    private Storage tier2 = null;
    private SegmentToContainerMapper segToConMapper;

    public StorageListSegmentsCommand(CommandArgs args) {
        super(args);
        segToConMapper = new SegmentToContainerMapper(getServiceConfig().getContainerCount());
    }
    @Override
    public void execute() throws Exception {
        ensureArgCount(1);
        String mnt = getCommandArgs().getArgs().get(0);
        FileSystemStorageConfig fsConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, mnt)
                .build();
        tier2 = new AsyncStorageWrapper(new RollingStorage(new FileSystemStorage(fsConfig), new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), createExecutorService(1));

        int containerCount = segToConMapper.getTotalContainerCount();
        FileWriter[] writers = new FileWriter[containerCount];
        for (int containerId=0; containerId < containerCount; containerId++) {
            File f = new File(String.valueOf(containerId));
            if(f.exists() && !f.delete()){
                System.err.println("Failed to delete "+ f.getAbsolutePath());
                System.exit(1);
            }
            if(!f.createNewFile()){
                System.err.println("Failed to create "+ f.getAbsolutePath());
                System.exit(1);
            }
            writers[containerId] = new FileWriter(f.getName());
        }
        System.out.println("Generating container files with the segments they own...");
        Iterator<SegmentProperties> it = tier2.listSegments().get();
        while(it.hasNext()) {
            SegmentProperties curr = it.next();
            //TODO: move this to server side?
            if(StreamSegmentNameUtils.isAttributeSegment(curr.getName()))
                continue;
            int containerId = segToConMapper.getContainerId(curr.getName());
            writers[containerId].write(curr.getLength()+"\t"+ curr.isSealed()+"\t"+curr.getName()+"\n");
        }
        for (int containerId=0; containerId < containerCount; containerId++) {
            writers[containerId].close();
        }
        System.out.println("Done!");
    }
    public static ScheduledExecutorService createExecutorService(int threadPoolSize) {
        ScheduledThreadPoolExecutor es = new ScheduledThreadPoolExecutor(threadPoolSize);
        es.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        es.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        es.setRemoveOnCancelPolicy(true);
        return es;
    }
    public static CommandDescriptor descriptor() {
        final String component = "storage";
        return new CommandDescriptor(component, "list-segments", "lists segments from tier-2 and displays their name, length, sealed status",
                new ArgDescriptor("root", "mount path"));
    }
}
