# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
import math

from model.constants import Constants
from model.provisioning_logic import *
from performance.performance_profiles import BareMetalCluster


def get_bool_input(msg):
    """
    Gets a user input from the console, ensuring that it is either 'yes' or 'no'.
    :param msg: Message to be prompted to the user.
    :return: Valid user input.
    """
    user_input = input(msg + " (yes, no)")
    while user_input != "yes" and user_input != "no":
        user_input = input(msg + " (yes, no)")
    return user_input == "yes"


def get_int_input(msg, valid_values):
    """
    Gets an integer user input from the console that should be contained in the list of valid values.
    :param msg: Message to be prompted to the user.
    :param valid_values: List of integer values, one of which is expected to be introduced by the user.
    :return: Valid integer value introduced by the user.
    """
    user_input = int(input(msg + "(valid values: " + str(valid_values) + ")"))
    while user_input not in valid_values:
        user_input = int(input(msg + "(valid values: " + str(valid_values) + ")"))
    return user_input


def get_vm_flavor():
    """
    Asks the user for the resources that each VM in the cluster will have.
    :return: Amount of CPU cores, GBs of RAM and local drives (if applies) per VM.
    """
    print("Please, introduce the resource information about the VMs used int cluster:")
    vm_cpus = int(input("How many CPU cores has each VM/node?"))
    vm_ram_gb = int(input("How much memory in GB has each VM/node?"))
    vm_local_drives = 0
    if get_bool_input("Is your cluster using local drives?"):
        vm_local_drives = int(input("How many local drives has each VM/node?"))
    return vm_cpus, vm_ram_gb, vm_local_drives


def provision_for_availability():
    """
    Gets the number of instances in the cluster for all services to tolerate the number of failures introduced by the
    user.
    :return: Number of Zookeeper, Bookkeeper, Segment Store and Controller instances to tolerate the specified failures.
    """
    failures_to_tolerate = int(input("How many instance/VM failures do you want the cluster to tolerate?"))
    zk_servers = calc_zk_servers_for_availability(failures_to_tolerate)
    bk_servers = calc_bookies_for_availability(failures_to_tolerate)
    ss_servers = calc_segment_stores_for_availability(failures_to_tolerate)
    cs_servers = calc_controllers_for_availability(failures_to_tolerate)
    return zk_servers, bk_servers, ss_servers, cs_servers


def get_requested_resources(zk_servers, bk_servers, ss_servers, cc_servers):
    """
    Calculates the total amount of CPU and RAM resources based on the number of instances of each type passed by
    parameter.
    :param zk_servers: Number of Zookeeper instances.
    :param bk_servers: Number of Bookkeeper instances.
    :param ss_servers: Number of Segment Stores.
    :param cc_servers: Number of Controllers.
    :return: Amount of CPU cores and RAM in GB required to allocate all the instances passed as input.
    """
    requested_ram_gb = calc_requested_pravega_ram_gb(zk_servers, bk_servers, ss_servers, cc_servers)
    print("Requested RAM GB: ", requested_ram_gb)
    requested_cpus = calc_requested_pravega_cpus(zk_servers, bk_servers, ss_servers, cc_servers)
    print("Requested CPUs: ", requested_cpus)
    return requested_cpus, requested_ram_gb


def resource_based_provisioning(vms, vm_cpus, vm_ram_gb, vm_local_drives, zookeeper_servers, bookkeeper_servers, segment_stores, controllers):
    can_allocate_cluster, the_cluster = can_allocate_services_on_nodes(vms, vm_cpus, vm_ram_gb, vm_local_drives,
                                                    zookeeper_servers, bookkeeper_servers, segment_stores, controllers)
    # First, make sure that whatever initial number of instances can be allocated, otherwise just throw an error.
    assert can_allocate_cluster, "Not even the minimal Pravega cluster can be allocated with the current resources"

    # Ask to the user the number of node failures he/she wants to tolerate.
    failures_to_tolerate = get_int_input("How many node failures you want to tolerate?", range(0, 100))
    if failures_to_tolerate >= vms:
        assert False, "You have not enough nodes to tolerate such amount of failures"
    elif vms - failures_to_tolerate < Constants.min_bookkeeper_servers or vms - failures_to_tolerate < Constants.min_zookeeper_servers:
        print("WARNING: In the case of experiencing all the failures specified, IOs may be interrupted until "
              "some processes are reallocated to alive nodes to reach the minimum quorum (e.g., Bookies, ZK servers).")

    # The nodes used for calculating Pravega cluster size are the nodes left in the worst case of failures. If we don't
    # do this and define the Pravega cluster based on all the nodes, then it won't be possible to reallocate pods to
    # remaining nodes as they would be full already. 
    vms = vms - failures_to_tolerate

    # Ask to the user whether this is a metadata-intensive workload or not.
    metadata_heavy_workload = get_bool_input("Is the workload metadata-heavy (i.e., many clients, small transactions)?")

    # Search for the maximum number of instances to saturate the cluster.
    while can_allocate_cluster:

        # Increase the number of Bookies first, if they keep a 1 to 1 relationship with Segment Stores.
        if bookkeeper_servers <= segment_stores:
            tentative_bookkeeper_servers = bookkeeper_servers + 1
            can_allocate_cluster, the_cluster = can_allocate_services_on_nodes(vms, vm_cpus, vm_ram_gb, vm_local_drives,
                                                zookeeper_servers, tentative_bookkeeper_servers, segment_stores, controllers)
        else: tentative_bookkeeper_servers = bookkeeper_servers

        # Try to increase the number of Segment Stores if possible.
        if can_allocate_cluster:
            bookkeeper_servers = tentative_bookkeeper_servers
            tentative_segment_stores = Constants.segment_stores_to_bookies_ratio(bookkeeper_servers)
            can_allocate_cluster, the_cluster = can_allocate_services_on_nodes(vms, vm_cpus, vm_ram_gb, vm_local_drives,
                                                zookeeper_servers, bookkeeper_servers, tentative_segment_stores, controllers)
        else: continue

        # Try to increase the number of Controllers if possible.
        if can_allocate_cluster:
            segment_stores = tentative_segment_stores
            tentative_controllers = Constants.controllers_to_segment_stores_ratio(segment_stores, metadata_heavy_workload)
            can_allocate_cluster, the_cluster = can_allocate_services_on_nodes(vms, vm_cpus, vm_ram_gb, vm_local_drives,
                                                zookeeper_servers, bookkeeper_servers, segment_stores, tentative_controllers)
        else: continue

        # Try to increase the number of Zookeeper servers if possible.
        if can_allocate_cluster:
            controllers = tentative_controllers
            tentative_zookeeper_servers = Constants.zookeeper_to_bookies_ratio(bookkeeper_servers)
            can_allocate_cluster, the_cluster = can_allocate_services_on_nodes(vms, vm_cpus, vm_ram_gb, vm_local_drives,
                                                            zookeeper_servers, bookkeeper_servers, segment_stores, controllers)
        else: continue

        if can_allocate_cluster:
            zookeeper_servers = tentative_zookeeper_servers

    # Get the resources used for the last possible allocation
    the_cluster = can_allocate_services_on_nodes(vms, vm_cpus, vm_ram_gb, vm_local_drives, zookeeper_servers,
                                                 bookkeeper_servers, segment_stores, controllers)[1]
    print("Allocation of pods on nodes: ", the_cluster)
    # Finally, we need to check how much memory is left in the nodes so we can share it across Segment Stores for cache.
    # In the worst case, we will have [math.ceil(vms/segment_stores)] Segment Store instances on a single node. Also,
    # in the worst case, this could be the node with the least available memory available. For this reason, the
    # in-memory cache size for a Segment Store would be as follows:
    min_vm_mem_available = min(mem for (cpu, mem, disks, processes_in_vm) in the_cluster)
    max_segment_stores_per_vm = math.ceil(vms / segment_stores)
    new_direct_memory = Constants.segment_store_direct_memory_in_gb + int(min_vm_mem_available/max_segment_stores_per_vm)
    print("--------- Segment Store In-Memory Cache Size (Pravega +0.7) ---------")
    print("Segment Store pod memory limit: ", Constants.segment_store_jvm_size_in_gb + new_direct_memory, "GB")
    print("Segment Store JVM Size (-Xmx JVM Option) : ", Constants.segment_store_jvm_size_in_gb, "GB")
    print("Segment Store direct memory (-DirectMemory JVM Option): ", new_direct_memory, "GB")
    # We leave 1GB extra of direct memory for non-caching purposes
    print("Segment Store cache size (pravegaservice.cacheMaxSize): ", (new_direct_memory - 1) * 1024 * 1024 * 1024,
          " (", new_direct_memory - 1, "GB)")
    print("Buffering time that Pravega tolerates with Tier 2 unavailable given a (well distributed) write workload:")
    for w in range(100, 1000, 200):
        print("- Write throughput: ", w, "(MBps) -> buffering time: ", ((segment_stores * (new_direct_memory - 1) * 1024) / w), " seconds")

    # Add a warning if the number of Bookies is higher than the number of nodes, as Pravega may need to enable rack-
    # aware placement in Bookkeeper so it tries to write to Bookies in different nodes (data availability).
    if bookkeeper_servers > vms:
        print("WARNING: To guarantee data availability and durability, consider enabling rack-aware placement in "
              "Pravega to write to Bookkeeper. Otherwise, Segment Stores may be writing to Bookies on the same node,"
              "which make the system more vulnerable to node failures.")

    return zookeeper_servers, bookkeeper_servers, segment_stores, controllers


def workload_based_provisioning(zookeeper_servers, bookkeeper_servers, segment_stores, controllers):
    # This sets the performance numbers of the Pravega services on a specific environment (e.g., bare metal, PKS).
    performance_profile = BareMetalCluster()

    # Provision Pravega data plane for workload (Bookkeeper, Segment Store).
    if get_bool_input("Do you want to right-size the Pravega 'data plane' based on the workload?"):
        write_events_per_second = int(input("How many events/second will the cluster handle?"))
        event_size = get_int_input("What is the expected typical size of events in bytes?", performance_profile.event_sizes)
        bookkeeper_servers = max(bookkeeper_servers, calc_bookies_for_workload(write_events_per_second, event_size,
                                                                               performance_profile))
        expected_latency = int(input("What is the expected 95th percentile latency in milliseconds for event writes?"))
        segment_stores = max(segment_stores, calc_segmentstores_for_latency(write_events_per_second, event_size,
                                                                            expected_latency, performance_profile))

    # Provision Pravega control plane for workload (Controller).
    if get_bool_input("Do you want to right-size the Pravega 'control plane' based on the workload?"):
        num_streams = int(input("How many Streams are expected to be stored in the system?"))

        # First, we characterize the Streams in the system, which generate background load to the Controllers.
        truncations_per_second = 0
        scales_per_second = 0
        if get_bool_input("Are Streams configured with a retention policy?"):
            truncations_per_second = int((int(input("Which is the typical Stream retention period in hours?")) / 3600.0) * num_streams)
        if get_bool_input("Are Streams configured with a scaling policy?"):
            scales_per_second = int((int(input("How many scales are typically expected in a Stream per hour?")) / 3600.0) * num_streams)

        # Second, we characterize the behavior of Clients.
        num_writers = int(input("How many Writers are expected to write data to Pravega?"))
        num_readers = int(input("How many Readers are expected to read data from Pravega?"))
        metadata_ops_per_second = performance_profile.writer_default_metadata_ops_per_second * num_writers + \
                                  performance_profile.reader_default_metadata_ops_per_second * num_readers
        transaction_operations_per_second = 0
        if get_bool_input("Are there clients executing other types of metadata operations? (e.g., REST, list streams, get Stream info)"):
            num_clients_extra_metadata_ops = int(input("How many clients are executing extra metadata operations?"))
            metadata_ops_per_second += int((int(input("For a single client, how many extra metadata operations per hour are expected?")) / 3600.0) * num_clients_extra_metadata_ops)
        if get_bool_input("Are writers working with Transactions?"):
            transactions_per_hour = int(input("For a single writer, how many Transactions per hour are expected?"))
            # At least, a transaction involves a create and a commit/abort operation, so we multiply by 2.
            transaction_operations_per_second = int((transactions_per_hour / 3600.0) * num_writers * 2)
            # Transactions also involve metadata operations related to keep-alives (e.g., Transaction pings). We assume
            # a pessimistic scenario in which all the transactions are open in parallel.
            metadata_ops_per_second += num_writers * transactions_per_hour * (1.0 / performance_profile.transaction_ping_period_in_seconds)

        # In the Controller, we distinguish between light (e.g., getSegments, createScope) and heavy (createStream,
        # commitTransaction) operations, as they exhibit very different performance and complexity.
        heavy_operations_per_second = truncations_per_second + scales_per_second + transaction_operations_per_second
        controllers = max(controllers, calc_controllers_for_workload(num_streams, heavy_operations_per_second,
                                                                     metadata_ops_per_second, performance_profile))

    return zookeeper_servers, bookkeeper_servers, segment_stores, controllers


def main():
    print("### Provisioning model for Pravega clusters ###")

    # First, get the type of VMs/nodes that will be used in the cluster.
    vm_cpus, vm_ram_gb, vm_local_drives = get_vm_flavor()

    # Initialize the number of instances with the minimum defaults.
    zookeeper_servers = Constants.min_zookeeper_servers
    bookkeeper_servers = Constants.min_bookkeeper_servers
    segment_stores = Constants.min_segment_stores
    controllers = Constants.min_controllers
    vms = 0

    # Initialize values for calculation of scaling policy.
    event_size = 0
    write_events_per_second = 0
    num_streams = 0

    provisioning_model = get_int_input("Do you want to provision a Pravega cluster based on 0) the resources available, "
                                       "1) the input workload, 2) none?", [0, 1, 2])

    if provisioning_model == 0:
        vms = int(input("How many VMs/nodes do you want to devote for a Pravega cluster?"))
        zookeeper_servers, bookkeeper_servers, segment_stores, controllers = resource_based_provisioning(vms, vm_cpus,
                        vm_ram_gb, vm_local_drives, zookeeper_servers, bookkeeper_servers, segment_stores, controllers)
    elif provisioning_model > 0:
        # Provision for data availability.
        if get_bool_input("Do you want to provision redundant instances to tolerate failures?"):
            # Calculate the number of instances of each type to tolerate the given number of failures.
            zookeeper_servers, bookkeeper_servers, segment_stores, controllers = provision_for_availability()
            vms = calc_min_vms_for_availability(zookeeper_servers, bookkeeper_servers, segment_stores, controllers)

        if provisioning_model == 1:
            zookeeper_servers, bookkeeper_servers, segment_stores, controllers = \
                workload_based_provisioning(zookeeper_servers, bookkeeper_servers, segment_stores, controllers)
        else:
            print("No provisioning model selected")

        # Get the number of VMs based on the necessary for the instances selected so far.
        requested_cpus, requested_ram_gb = get_requested_resources(zookeeper_servers, bookkeeper_servers, segment_stores, controllers)
        vms = max(vms, calc_min_vms_for_resources(vm_ram_gb, vm_cpus, requested_ram_gb, requested_cpus))

    # Calculate the number of containers and buckets.
    num_containers = Constants.segment_containers_per_segment_store * segment_stores
    num_buckets = Constants.stream_buckets_per_controller * controllers

    # Estimate an appropriate value for the Scaling Policy of Stream in this environment. At this point, we assume that
    # the user has some workload requirements (e.g., latency, load), and the number of Segment Stores (and therefore, of
    # containers) is derived to meet that goal. Then, we just calculate a trigger rate for Streams that tries to exploit
    # the parallelism of the cluster (i.e., one segment per container).
    stream_scaling_suggested_value = 0
    if num_streams != 0 and write_events_per_second != 0:
        # First, we calculate for a single stream, the rate in events per second to spread across all the Segment
        # Containers. The goal is to achieve good load balancing.
        stream_scaling_suggested_value = (write_events_per_second / num_streams) / num_containers
        # On the one hand, we need to make sure that for a single Stream, the rate is not too low, as it would induce a
        # too high metadata overhead in the cluster without improving load balancing significantly.
        stream_scaling_suggested_value = min(stream_scaling_suggested_value,
                                             performance_profile.segment_scaling_max_events[event_size])
        # On the other hand, we also need to set some upper limit to the scaling trigger, as otherwise a single Segment
        # Store may get saturated.
        stream_scaling_suggested_value = max(stream_scaling_suggested_value,
                                             performance_profile.segment_scaling_min_events[event_size])

    # Output the cluster estimation result.
    print("--------- Cluster Provisioning ---------")
    print("Number of VMs required: ", vms)
    print("Number of Zookeeper servers required: ", zookeeper_servers)
    print("Number of Bookkeeper servers required: ", bookkeeper_servers)
    print("Number of Segment Stores servers required: ", segment_stores)
    print("Number of Controller servers required: ", controllers)
    print("--------- Cluster Config Params ---------")
    print("Number of Segment Containers in config: ", num_containers)
    print("Number of Stream Buckets in config: ", num_buckets)
    # Only print suggested scaling policy if we have provided some information about workload.
    if stream_scaling_suggested_value != 0:
        print("--------- Stream Scaling Policy ---------")
        print("Suggested Stream Scaling Policy trigger (events/second): ", int(stream_scaling_suggested_value))


if __name__ == '__main__':
    main()
