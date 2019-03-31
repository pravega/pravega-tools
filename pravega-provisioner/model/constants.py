# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#


class Constants(object):
    # Minimum Pravega Cluster deployment to preserve data durability (3-way replication). Nothing less than this should
    # be considered for non-testing deployments.
    min_zookeeper_servers = 3
    min_bookkeeper_servers = 3
    min_segment_stores = 1
    min_controllers = 1

    # Recommended resources to be provisioned per Pravega cluster instance.
    zk_server_ram_gb = 2
    zk_server_cpus = 1
    bookie_ram_gb = 4
    bookie_cpus = 2
    controller_ram_gb = 3
    controller_cpus = 2
    segment_store_ram_gb = 6
    segment_store_cpus = 4

    # Number of segment containers and buckets per Segment Store and Controller, respectively.
    segment_containers_per_segment_store = 8
    stream_buckets_per_controller = 4


