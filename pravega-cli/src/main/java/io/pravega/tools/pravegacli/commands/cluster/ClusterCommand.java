/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.tools.pravegacli.commands.cluster;

import io.pravega.controller.store.stream.ZKStoreHelper;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.tools.pravegacli.commands.Command;
import io.pravega.tools.pravegacli.commands.CommandArgs;
import java.util.concurrent.ForkJoinPool;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.curator.framework.CuratorFramework;

abstract class ClusterCommand extends Command {
    protected static final String COMPONENT = "cluster";

    ClusterCommand(CommandArgs args) {
        super(args);
    }


    /**
     * Creates a new Context to be used by the BookKeeper command.
     *
     * @return A new Context.
     * @throws DurableDataLogException If the BookKeeperLogFactory could not be initialized.
     */
    protected Context createContext() {
        CuratorFramework curatorFramework = createZKClient();
        ZKStoreHelper zkStoreHelper = new ZKStoreHelper(curatorFramework, ForkJoinPool.commonPool());
        return new Context(curatorFramework, zkStoreHelper);
    }

    @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
    protected static class Context implements AutoCloseable {
        final CuratorFramework zkClient;
        final ZKStoreHelper zkStoreHelper;

        @Override
        public void close() {
            this.zkClient.close();
        }
    }
}
