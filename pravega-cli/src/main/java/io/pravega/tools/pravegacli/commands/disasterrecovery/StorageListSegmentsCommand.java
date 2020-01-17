package io.pravega.tools.pravegacli.commands.disasterrecovery;

import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;

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


public class StorageListSegmentsCommand extends Command {

    public StorageListSegmentsCommand(CommandArgs args) {
        super(args);
    }
    protected static final String APPEND_FORMAT = "Segment_%s_Append_%d";
    protected static final long DEFAULT_ROLLING_SIZE = (int) (APPEND_FORMAT.length() * 1.5);
    private static Storage tier2 = null;

    @Override
    public void execute() throws Exception {
        ensureArgCount(1);
        String mnt = getCommandArgs().getArgs().get(0);
        FileSystemStorageConfig fsConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, mnt)
                .build();
        tier2 = new AsyncStorageWrapper(new RollingStorage(new FileSystemStorage(fsConfig), new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), createExecutorService(1));

        //TODO: write output about storage
        System.out.println("segments from tier-2");
        System.out.println("======================================================");
        Iterator<SegmentProperties> it = tier2.listSegments().get();
        System.out.println("Length\t\t\tisSealed\t\t\tPath");
        System.out.println("======================================================");
        while(it.hasNext()) {
            SegmentProperties curr = it.next();
            System.out.println(curr.getLength()+"\t\t\t"+ curr.isSealed()+"\t\t\t"+curr.getName());
        }
        System.out.println("======================================================");
    }
    static ScheduledExecutorService createExecutorService(int threadPoolSize) {
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
