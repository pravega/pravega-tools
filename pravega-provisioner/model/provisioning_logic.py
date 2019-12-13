# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
from model.constants import Constants


def calc_zk_servers_for_availability(failures_to_tolerate):
    """
    This method calculates the minimum number of vms in a Zookeeper servers to tolerate a given amount of failures.
    """
    assert failures_to_tolerate >= 0, "Negative failures to tolerate not allowed."
    zk_servers = Constants.min_zookeeper_servers + failures_to_tolerate
    # We need to provision an odd number of Zookeeper instances.
    if zk_servers % 2 == 0:
        zk_servers -= 1
    return zk_servers


def calc_bookies_for_availability(failures_to_tolerate):
    """
    This method calculates the minimum number of vms in a Bookkeeper servers to tolerate a given amount of failures.
    """
    assert failures_to_tolerate >= 0, "Negative failures to tolerate not allowed."
    return Constants.min_bookkeeper_servers + failures_to_tolerate


def calc_controllers_for_availability(failures_to_tolerate):
    """
    This method calculates the minimum number of vms in a Controller servers to tolerate a given amount of failures.
    """
    assert failures_to_tolerate >= 0, "Negative failures to tolerate not allowed."
    return Constants.min_controllers + failures_to_tolerate


def calc_segment_stores_for_availability(failures_to_tolerate):
    """
    This method calculates the minimum number of vms in a Segment Store servers to tolerate a given amount of failures.
    """
    assert failures_to_tolerate >= 0, "Negative failures to tolerate not allowed."
    return Constants.min_segment_stores + failures_to_tolerate


def calc_requested_pravega_ram_gb(zk_servers, bookies, segment_stores, controllers):
    """
    Calculates the amount of RAM in GBs required to allocate the instances of the different services passed as input.
    :param zk_servers: Number of Zookeeper servers.
    :param bookies: Number of Bookkeeper servers.
    :param segment_stores: Number of Segment Stores.
    :param controllers: Number of Controllers.
    :return: Amount of RAM in GBs to allocate all the services.
    """
    return zk_servers * Constants.zk_server_ram_gb + bookies * Constants.bookie_ram_gb + \
           segment_stores * Constants.segment_store_ram_gb + controllers * Constants.controller_ram_gb


def calc_requested_pravega_cpus(zk_servers, bookies, segment_stores, controllers):
    """
    Calculates the amount of CPU cores required to allocate the instances of the different services passed as input.
    :param zk_servers: Number of Zookeeper servers.
    :param bookies: Number of Bookkeeper servers.
    :param segment_stores: Number of Segment Stores.
    :param controllers: Number of Controllers.
    :return: Amount of CPU cores to allocate all the services.
    """
    return zk_servers * Constants.zk_server_cpus + bookies * Constants.bookie_cpus + \
           segment_stores * Constants.segment_store_cpus + controllers * Constants.controller_cpus


def calc_min_vms_for_resources(vm_ram, vm_cpu, requested_ram_gb, requested_cpus):
    """
    Return the minimum number of VMs of a specific flavour (X RAM in GBs, Y CPU cores per VM) requires to allocate the
    amount of RAM in GBs and CPU cores passed as input.
    :param vm_ram: RAM in GBs per VM.
    :param vm_cpu: CPU cores per VM.
    :param requested_ram_gb: Amount of RAM in GB to allocate.
    :param requested_cpus: Amount of CPU cores to allocate.
    :return: Minimum necessary number of VMs with to satisfy the requested resources.
    """
    return max(int(requested_ram_gb/vm_ram), int(requested_cpus/vm_cpu)) + 1


def calc_min_vms_for_availability(zk_servers, bk_servers, ss_servers, cc_servers):
    """
    This method assumes as input a number of instances per service to tolerate a given number of failures. With this,
    we calculate the number of VMs to respect the same failure tolerance, which translates into the maximum number of
    instances of any type. This is because having fewer VMs yield that a single VM failure would induce multiple
    failures in the Pravega services, making the failure tolerance guarantees ineffective.
    :param zk_servers: Number of Zookeeper instances.
    :param bk_servers: Number of Bookkeeper instances.
    :param ss_servers: Number of Segment Stores.
    :param cc_servers: Number of Controllers.
    :return: Minimum number of VMs to satisfy the failure tolerance requirements.
    """
    return max(zk_servers, bk_servers, ss_servers, cc_servers)


def calc_bookies_for_workload(write_events_per_second, event_size, performance_profile):
    """
    Given the Bookkeeper throughput saturation points provided in the performance profile, we derive the necessary
    number of Bookies to effectively absorb a given write workload.
    :param write_events_per_second: Number of write events per second in the target workload.
    :param event_size: Typical event size.
    :param performance_profile: Class containing the performance of Bookkeeper in the target environment.
    :return: Number of Bookies to absorb the target workload.
    """
    bookie_saturation_point_for_event_size = 0
    for (es, events_sec) in performance_profile.bookie_saturation_points:
        # We analyzed some pre-defined event sizes.
        if event_size == es:
            bookie_saturation_point_for_event_size = events_sec
            break
    return int(write_events_per_second / bookie_saturation_point_for_event_size + 1)


def calc_segmentstores_for_latency(write_events_per_second, event_size, expected_max_latency, performance_profile):
    """
    Given the Segment Store throughput vs latency trade-off provided in the performance profile, we derive the necessary
    number of instances to effectively absorb a given write workload and guarantee an expected latency (e.g., 95th perc.).
    :param write_events_per_second: Number of write events per second in the target workload.
    :param event_size: Typical event size.
    :param expected_max_latency: Expected max latency of events in the 95th percentile.
    :param performance_profile: Class containing the performance of the Segment Store in the target environment.
    :return: Number of Segment Store instances to absorb the target workload respecting the latency requirements.
    """
    for (es, throughput_vs_latency) in performance_profile.segmentstore_latency_profile:
        # We analyzed some pre-defined event sizes.
        if event_size == es:
            for (throughput, latency) in sorted(throughput_vs_latency, reverse=True):
                # Once we reach the desired max latency value, calculate the number of instances.
                if latency < expected_max_latency:
                    return int(write_events_per_second / throughput + 1)
    raise Exception("Could not meet expected latency for Segment Store operations.")


def calc_controllers_for_workload(num_streams, heavy_operations_per_second, light_operations_per_second, performance_profile):
    """
    Given the Controller throughput for the different operation types provided in the performance profile, we derive the
    necessary number of Controllers to handle the target metadata workload.
    :param num_streams: Expected number of Streams in the system.
    :param heavy_operations_per_second: Heavy (Stream, Transaction) operations per second.
    :param light_operations_per_second: Light (Scope, Ping, Endpoints, Segments) operations per second.
    :param performance_profile:  Class containing the performance of the Controller in the target environment.
    :return: Number of Controller instances to absorb the target metadata workload.
    """
    return int(max(num_streams / performance_profile.max_streams_per_controller + 1,
               heavy_operations_per_second / performance_profile.controller_max_heavy_operations_per_second + 1,
               light_operations_per_second / performance_profile.controller_max_light_operations_per_second + 1))


def can_allocate_services_on_nodes(vms, vm_cpus, vm_ram_gb, zookeeper_servers, bookkeeper_servers, segment_stores, controllers):
    the_cluster = list()
    # Initialize the cluster modelling the resources available (VM_CPUS, VM_RAM).
    for i in range(vms):
        the_cluster.append((vm_cpus, vm_ram_gb))

    # Allocate the different instances in a round robin fashion across nodes to maximize failure tolerance.
    # Start allocating Bookies
    if not _subtract_resources_from_cluster(the_cluster, bookkeeper_servers, Constants.bookie_cpus, Constants.bookie_ram_gb):
        return False, the_cluster
    # Then, attempt to allocate Segment Stores
    if not _subtract_resources_from_cluster(the_cluster, segment_stores, Constants.segment_store_cpus, Constants.segment_store_ram_gb):
        return False, the_cluster
    # Attempt to allocate Controllers
    if not _subtract_resources_from_cluster(the_cluster, controllers, Constants.controller_cpus, Constants.controller_ram_gb):
        return False, the_cluster
    # Attempt to allocate Zookeeper servers
    if not _subtract_resources_from_cluster(the_cluster, zookeeper_servers, Constants.zk_server_cpus, Constants.zk_server_ram_gb):
        return False, the_cluster

    # Allocation of all the instances has been possible.
    return True, the_cluster


def _subtract_resources_from_cluster(the_cluster, num_instances, cpu_per_instance, ram_per_instance):
    finish_allocation = False
    completed_allocations = retries = 0
    perfect_instance_distribution_across_nodes = True
    while not finish_allocation:
        for x in range(num_instances - completed_allocations):
            [old_cpu, old_ram] = the_cluster[x % len(the_cluster)]
            # Only allocate when there are enough resources
            if (old_cpu - cpu_per_instance) >= 0 and (old_ram - ram_per_instance) >= 0:
                the_cluster[x % len(the_cluster)] = (old_cpu - cpu_per_instance, old_ram - ram_per_instance)
                completed_allocations += 1
        # After allocating instances, sort nodes based on available resources.
        the_cluster.sort(key=lambda tup: tup[0], reverse=True)

        if num_instances == completed_allocations:
            finish_allocation = True
        if retries > 0:
            perfect_instance_distribution_across_nodes = False
        if retries >= len(the_cluster):
            return False
        retries += 1

    if not perfect_instance_distribution_across_nodes:
        print("WARNING: It is not possible to perfectly distribute instances across nodes, which will impact on the"
              " failure tolerance of the cluster.")

    return True


