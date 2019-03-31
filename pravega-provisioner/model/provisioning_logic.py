# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

from constants import Constants


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
    return zk_servers * Constants.zk_server_ram_gb + bookies * Constants.bookie_ram_gb + \
           segment_stores * Constants.segment_store_ram_gb + controllers * Constants.controller_ram_gb


def calc_requested_pravega_cpus(zk_servers, bookies, segment_stores, controllers):
    return zk_servers * Constants.zk_server_cpus + bookies * Constants.bookie_cpus + \
           segment_stores * Constants.segment_store_cpus + controllers * Constants.controller_cpus


def calc_min_vms_for_resources(vm_ram, vm_cpu, requested_ram_gb, requested_cpus):
    return max(int(requested_ram_gb/vm_ram), int(requested_cpus/vm_cpu)) + 1


def calc_min_vms_for_availability(zk_servers, bk_servers, ss_servers, cc_servers):
    return max(zk_servers, bk_servers, ss_servers, cc_servers)


def calc_bookies_for_workload(write_events_per_second, event_size, performance_profile):
    bookie_saturation_point_for_event_size = 0
    for (es, events_sec) in performance_profile.bookie_saturation_points:
        if event_size <= es:
            bookie_saturation_point_for_event_size = events_sec
            break
    return write_events_per_second / bookie_saturation_point_for_event_size + 1


def calc_segmentstores_for_latency(write_events_per_second, event_size, expected_latency, performance_profile):
    for (es, throughput_vs_latency) in performance_profile.segmentstore_latency_profile:
        if event_size <= es:
            for (throughput, latency) in sorted(throughput_vs_latency, reverse=True):
                if latency < expected_latency:
                    return write_events_per_second / throughput + 1
    raise Exception("Could not meet expected latency for Segment Store operations.")


def calc_controllers_for_workload(num_streams, heavy_operations_per_second, light_operations_per_second, performance_profile):
    return max(num_streams / performance_profile.max_streams_per_controller + 1,
               heavy_operations_per_second / performance_profile.controller_max_heavy_operations_per_second + 1,
               light_operations_per_second / performance_profile.controller_max_light_operations_per_second + 1)

