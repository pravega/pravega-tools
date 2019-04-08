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

import io.pravega.tools.pravegacli.commands.CommandArgs;
import io.pravega.tools.pravegacli.commands.utils.ZKHelper;
import lombok.Cleanup;

/**
 * Outputs all the Controller, Segment Store and Bookkeeper instances in the system.
 */
public class GetClusterNodesCommand extends ClusterCommand {

    public GetClusterNodesCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(0);

        try {
            @Cleanup
            ZKHelper zkStoreHelper = ZKHelper.create(getServiceConfig().getZkURL(), getServiceConfig().getClusterName());
            output("Cluster name: " + getServiceConfig().getClusterName());
            output("Controller instances in the cluster:");
            zkStoreHelper.getControllers().forEach(c -> output("> " + c));
            output("Segment Store instances in the cluster:");
            zkStoreHelper.getSegmentStores().forEach(ss -> output("> " + ss));
            output("Bookies in the cluster:");
            zkStoreHelper.getBookies().forEach(b -> output("> " + b));
        } catch (Exception e) {
            System.err.println("Exception accessing to Zookeeper cluster metadata.");
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list-instances", "Lists all nodes in the Pravega " +
                "cluster (Controllers, Segment Stores).");
    }
}
