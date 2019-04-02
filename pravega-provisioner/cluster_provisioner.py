# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

from model.provisioning_logic import *
from performance.performance_profiles import BareMetalCluster


def get_bool_input(msg):
    """
    Gets a user input from the console, ensuring that it is either 'yes' or 'no'.
    :param msg: Message to be prompted to the user.
    :return: Valid user input.
    """
    user_input = raw_input(msg + " (yes, no)")
    while user_input != "yes" and user_input != "no":
        user_input = raw_input(msg + " (yes, no)")
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
    :return: Amount of CPU cores and GBs of RAM per VM.
    """
    print "Please, introduce the resource information about the VMs used int cluster:"
    vm_cpus = int(input("How many CPU cores has each VM/node?"))
    vm_ram_gb = int(input("How much memory in GB has each VM/node?"))
    return vm_cpus, vm_ram_gb


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
    print "Requested RAM GB: ", requested_ram_gb
    requested_cpus = calc_requested_pravega_cpus(zk_servers, bk_servers, ss_servers, cc_servers)
    print "Requested CPUs: ", requested_cpus
    return requested_cpus, requested_ram_gb


def main():
    print "### Provisioning model for Pravega clusters ###"

    # This sets the performance numbers of the Pravega services on a specific environment (e.g., bare metal, PKS).
    performance_profile = BareMetalCluster()

    # First, get the type of VMs/nodes that will be used in the cluster.
    vm_cpus, vm_ram_gb = get_vm_flavor()

    # Initialize the number of instances with the minimum defaults.
    zookeeper_servers = Constants.min_zookeeper_servers
    bookkeeper_servers = Constants.min_bookkeeper_servers
    segment_stores = Constants.min_segment_stores
    controllers = Constants.min_controllers
    vms = 0

    # Provision for data availability.
    if get_bool_input("Do you want to provision redundant instances to tolerate failures?"):
        # Calculate the number of instances of each type to tolerate the given number of failures.
        zookeeper_servers, bookkeeper_servers, segment_stores, controllers = provision_for_availability()
        vms = calc_min_vms_for_availability(zookeeper_servers, bookkeeper_servers, segment_stores, controllers)

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
        num_clients = int(input("How many Clients are expected to work with Pravega? (e.g., readers, writers)"))
        metadata_ops_per_second = performance_profile.client_default_metadata_ops_per_second * num_clients
        transaction_operations_per_second = 0
        if get_bool_input("Are the clients executing metadata operations? (e.g., REST, list streams, get Stream info)"):
            metadata_ops_per_second += int((int(input("For a single client, how many metadata operations per hour are expected?")) / 3600.0) * num_clients)
        if get_bool_input("Are clients working with Transactions?"):
            # At least, a transaction involves a create and a commit operation, so we multiply by 2.
            transaction_operations_per_second = int((int(input("For a single client, how many Transactions per hour are expected?")) / 3600.0) * num_clients * 2)
            # Transactions also involve more metadata operations (e.g., Transaction pings).
            metadata_ops_per_second *= 2

        # In the Controller, we distinguish between light (e.g., getSegments, createScope) and heavy (createStream,
        # commitTransaction) operations, as they exhibit very different performance and complexity.
        heavy_operations_per_second = truncations_per_second + scales_per_second + transaction_operations_per_second
        controllers = max(controllers, calc_controllers_for_workload(num_streams, heavy_operations_per_second,
                                                                     metadata_ops_per_second, performance_profile))

    # Estimate the amount of resources required to allocate all the service instances.
    requested_cpus, requested_ram_gb = get_requested_resources(zookeeper_servers, bookkeeper_servers, segment_stores,
                                                               controllers)
    vms = max(vms, calc_min_vms_for_resources(vm_ram_gb, vm_cpus, requested_ram_gb, requested_cpus))

    # Output the cluster estimation result.
    print "--------- Cluster Provisioning ---------"
    print "Minimum number of VMs required: ", vms
    print "Minimum number of Zookeeper servers required: ", zookeeper_servers
    print "Minimum number of Bookkeeper servers required: ", bookkeeper_servers
    print "Minimum number of Segment Stores servers required: ", segment_stores
    print "Minimum number of Controller servers required: ", controllers
    print "--------- Cluster Config Params ---------"
    print "Number of Segment Containers in config: ", Constants.segment_containers_per_segment_store * segment_stores
    print "Number of Stream Buckets in config: ", Constants.stream_buckets_per_controller * controllers


if __name__ == '__main__':
    main()
