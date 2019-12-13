# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
import math


class Constants:
    """
    Constant values and assumptions for the Pravega cluster provisioning model.
    """
    # Minimum Pravega Cluster deployment to preserve data durability (3-way replication). Nothing less than this should
    # be considered for non-testing deployments.
    min_zookeeper_servers = 3
    min_bookkeeper_servers = 3
    min_segment_stores = 1
    min_controllers = 1

    # Recommended resources to be provisioned per Pravega cluster instance.
    zk_server_ram_gb = 2
    zk_server_cpus = 1
    bookie_ram_gb = 16
    bookie_cpus = 8
    controller_ram_gb = 4
    controller_cpus = 2
    segment_store_ram_gb = 16
    segment_store_cpus = 8

    # Number of segment containers and buckets per Segment Store and Controller, respectively.
    segment_containers_per_segment_store = 8
    stream_buckets_per_controller = 4


    @staticmethod
    def zookeeper_to_bookies_ratio(bookies):
        """
        Number of Zookeeper instances based on the number of Bookies in the system, as Bookkeeper is the service that
        makes a more intensive use of Zookeeper managing metadata (more than the Controller). For large deployments
        e.g., > 20 Bookies), we keep a ratio of Zookeeper servers to Bookies of 1:4.
        """
        zk_instances = math.ceil(bookies / 4)
        if zk_instances < Constants.min_zookeeper_servers:
            return Constants.min_zookeeper_servers
        elif zk_instances / 2 == 0:
            # We should keep an odd number of Zookeeper servers.
            return zk_instances + 1
        else:
            return zk_instances


    @staticmethod
    def segment_stores_to_bookies_ratio(bookies):
        """
        In our benchmarks, we observe that we require the one Segment Store per Bookie to get full saturation of fast
        drives. Therefore, we define a 1:1 ratio between Bookies and Segment Stores.
        """
        return bookies


    @staticmethod
    def controllers_to_segment_stores_ratio(segment_stores, metadata_heavy_workload):
        """
        Unless the workload at hand is very metadata intensive (i.e., many clients, very small transactions), we can
        keep a 1:3 ratio between Controllers and Segment Stores. Otherwise, we can switch to a 1:2 ratio
        """
        ratio = 3
        if metadata_heavy_workload:
            ratio = 2
        controllers = segment_stores / ratio
        if controllers < Constants.min_controllers:
            return Constants.min_controllers
        else:
            return math.ceil(controllers)

