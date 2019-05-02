# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#


class BareMetalCluster(object):
    """
    Set of performance profiles from benchmarks in a bare metal cluster.
    """
    # Considered event sizes in our performance profiles.
    event_sizes = [100, 1000, 10000, 250000]
    '''
    Event size      Saturation point (3 Bookies)      Saturation point (6 Bookies)
    100B            60000 events/sec.                 120000 events/sec.
    1KB             60000 events/sec.                 120000 events/sec.
    10KB            12000 events/sec.                 24000 events/sec.
    250KB           800 events/sec.                   1600 events/sec.
    '''
    bookie_saturation_points = [(100, 20000), (1000, 20000), (10000, 4000), (250000, 267)]
    '''
    Number of operations and 95th latency in milliseconds depending on the event size for the Segment Store.
    '''
    segmentstore_latency_profile = [(100, [(50, 9), (250, 5), (500, 5), (1000, 7), (2500, 11), (5000, 26), (10000, 24),
                                           (15000, 20), (20000, 22), (25000, 21), (30000, 22), (50000, 21), (60000, 21),
                                           (70000, 22), (100000, 21), (150000, 18), (200000, 18), (250000, 17),
                                           (300000, 17)]),
                                    (1000, [(50, 5), (250, 4), (500, 6), (1000, 7), (5000, 27), (10000, 29), (15000, 31),
                                            (20000, 32), (30000, 33), (40000, 54), (50000, 56), (60000, 64), (70000, 72),
                                            (80000, 91), (90000, 95), (100000, 106)]),
                                    (10000, [(50, 5), (100, 15), (200, 20), (500, 21), (1000, 24), (2500, 71), (5000, 71),
                                             (7500, 101), (10000, 141), (12500, 166), (15000, 263)]),
                                    (250000, [(10, 32), (50, 23), (100, 28), (200, 21), (300, 79), (400, 81), (500, 95),
                                              (600, 97), (700, 121), (800, 152), (900, 162), (1000, 252)])]
    '''
    To suggest scaling policies for Streams, we also apply some bounds on the max and min number of events per second
    per segment. That is, a too low number of events per second may lead to an unnecessary high number of parallel
    segments that would induce high metadata overhead without any benefit in terms of load balancing. On the other hand,
    a too high value may lead to poor load balancing and to saturate Segment Store instances. Based on the previous
    benchmarks, we select some reasonable min/max values to bound the calculation of the scaling policy for a Stream. 
    '''
    segment_scaling_min_events = {100: 250, 1000: 25, 10000: 25, 250000: 5}
    segment_scaling_max_events = {100: 10000, 1000: 5000, 10000: 5000, 250000: 250}

    '''
    In the Controller, We distinguish between light operations (e.g., create/delete scopes, getSegments, getEndpoint)
    and heavy operations (e.g., create/delete Streams, scale streams, transactions management). We provide (pessimistic) 
    throughput values for operations belonging to each type extracted from real benchmarks against a Controller instance.
    '''
    controller_max_light_operations_per_second = 520
    controller_max_heavy_operations_per_second = 60
    '''
    Set a safe maximum of Streams to be managed by a single Controller instance.
    '''
    max_streams_per_controller = 10000
    '''
    Estimate of the number of automatic metadata operation that a client (writer/reader) may perform while writing or
    reading data from Pravega (getNextSegments, getEndpoint, pingTransaction, getDelegateToken, etc.). In general, these
    operations have been measured to be < 1 per second (occur upon disconnections, change in segments, etc.).
    '''
    writer_default_metadata_ops_per_second = 1
    reader_default_metadata_ops_per_second = 1
    '''Transaction ping period in seconds'''
    transaction_ping_period_in_seconds = 30
