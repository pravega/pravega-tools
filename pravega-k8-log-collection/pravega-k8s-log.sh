#!/usr/bin/env bash
#
# Pravega K8 based log collection tool
#
# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
set -euo pipefail
# Setting up temporary log collection directory under /tmp/logdir
mkdir -p /tmp/logdir
output_dir="/tmp/logdir"
# Capturing pod, services and persistentvolumeclaim information from all namespaces
kubectl get po,svc,pvc --all-namespaces > $output_dir/kubectl-get-po-svc-pvc-all-namespace_output
IFS=$'\n'
# Filter for Pravega only spaces (could be enhanced if other spaces need inclusion e.g. Nautilus)
for i in $(cat $output_dir/kubectl-get-po-svc-pvc-all-namespace_output | grep -i pravega)
do
echo $i
# Detect Pravega pods information in various K8 deployment (e.g. PKS / GKE)
if [[ $i == *"po/"* ]] || [[ $i == *"pod/"* ]]; then
# Collect logs from "Running" pods, if pods are not in "Running" state it will collect describe information
    if [ "`echo $i | awk '{print $4}'`" == "Running" ]; then
    {
# Detect if pod had been restarted and collect previous instance of the pod log if it exists
        if [ `echo $i | awk '{print $5}'` != 0 ]; then     
            kubectl -n `echo $i | awk '{print $1}'` logs `echo $i | awk '{print $2}'` -p > $output_dir/log_`echo $i | awk 'gsub(/\//,"_") {print $2}'`_previous.log
        fi
# Collect recent logs from pod
        kubectl -n `echo $i | awk '{print $1}'` logs `echo $i | awk '{print $2}'` --all-containers > $output_dir/log_`echo $i | awk 'gsub(/\//,"_") {print $2}'`_recent.log
# Detect if the pod have older logs due to logroate and collect all logs from that pod
        log_count=$(kubectl -n `echo $i | awk '{print $1}'` exec -it `echo $i |  awk '-F'[/] '{print $2}'|awk '{print $1}'` -- ls -l /opt/pravega/logs 2>/dev/null | wc -l || true)
        if [ $log_count -ge 2 ]; then  
            mkdir -p $output_dir/oldlogs_`echo $i |  awk '-F'[/] '{print $2}'|awk '{print $1}'` 
            kubectl -n `echo $i | awk '{print $1}'` cp `echo $i |  awk '-F'[/] '{print $2}'|awk '{print $1}'`:/opt/pravega/logs $output_dir/oldlogs_`echo $i |  awk '-F'[/] '{print $2}'|awk '{print $1}'`/.
        fi    
    }
    else
       echo "`echo $i | awk '{print $2}'` is not in running state"  
    fi
fi
# Collect details of resources information (pod, services, persistentvolumeclaim) for all resource phases (e.g. "Running", "Evicted")
kubectl -n `echo $i | awk '{print $1}'` describe `echo $i | awk '{print $2}'` > $output_dir/describe_`echo $i | awk 'gsub(/\//,"_") {print $2}'`_output
done
cwd=`pwd`
mkdir -p $cwd/pravega-k8s-logs
cd $output_dir
# Create compressed log archive with date stamp
tar zcf $cwd/pravega-k8s-logs/pravega-k8s-logging-`date '+%Y-%m-%d-%H-%M-%S'`.tgz *
rm -rf $output_dir
echo "Final logs are avaialble in `ls $cwd/pravega-k8s-logs/*.tgz`"
