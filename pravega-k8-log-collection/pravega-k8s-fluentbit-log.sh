#/usr/bin/env bash
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
output_dir="./logdir"
mkdir -p $output_dir
# Capturing pod, services and persistentvolumeclaim information from all namespaces
kubectl get po,svc,pvc --all-namespaces > $output_dir/kubectl-get-po-svc-pvc-all-namespace_output
IFS=$'\n'
# We now rok only on the "fluentbit" pods that contain the logs of the rest of services.
for i in $(cat $output_dir/kubectl-get-po-svc-pvc-all-namespace_output | grep -i fluent-bit)
do

    # Detect Pravega pods information in various K8 deployment (e.g. PKS / GKE)
    if [[ $i == *"po/"* ]] || [[ $i == *"pod/"* ]]; then

        fluentbit_pod=$(echo $i |  awk '-F'[/] '{print $2}'| awk '{print $1}')
        # Collect logs from "Running" pods, if pods are not in "Running" state it will collect describe information.
        if [ "`echo $i | awk '{print $4}'`" == "Running" ]; then
            namespace=$(echo $i | awk '{print $1}')
            # Clean any pre-existing tar file in the fluentbit pod.
            kubectl -n $namespace exec $fluentbit_pod -- bash -c "rm /tmp/$fluentbit_pod.tar.gz" || true
            # Create a tar.gz file with all the logs contained in the fluentbit pod.
            exitcode=0
            kubectl -n $namespace exec $fluentbit_pod -- bash -c "tar -czf /tmp/$fluentbit_pod.tar.gz /var/vcap/store/docker/docker/containers" || exitcode=$?
            # Some logs may be being written during collection, which makes tar to exit with exit code 1. Need to handle this case.
            if [ "$exitcode" != "1" ] && [ "$exitcode" != "0" ]; then
                    exit $exitcode
            fi
            # Download the compressed file from the fluentbit pod.
            kubectl cp $namespace/$fluentbit_pod:/tmp/$fluentbit_pod.tar.gz $output_dir/$fluentbit_pod.tar.gz
            # Remove temporal compressed file from fluentbit pod.
            kubectl -n $namespace exec $fluentbit_pod -- bash -c "rm /tmp/$fluentbit_pod.tar.gz"
            # Untar the contents to provide a better structure of the resulting logs artifact.
            tar -xzf $output_dir/$fluentbit_pod.tar.gz -C $output_dir
            mv $output_dir/var/vcap/store/docker/docker/containers/ $output_dir/$fluentbit_pod
            # Delete useless files from downloaded fluentbit dirs (preserve only .log files and the directory structure).
            for container_dir in $(find $output_dir/$fluentbit_pod -maxdepth 1 -mindepth 1 -type d)
            do
                    echo $container_dir
                    mv $container_dir/*.log $container_dir/..
                    rm -rf $container_dir/*
                    mv $container_dir/../*.log $container_dir
            done
            # Relate the name of the pods being logged with the actual log data.
            kubectl -n $namespace exec $fluentbit_pod -- ls /var/vcap/store/docker/docker/containers/ > $output_dir/container-lognames
            kubectl -n $namespace exec $fluentbit_pod -- ls /var/log/containers/ > $output_dir/pod-lognames
            for podlog in $(cat $output_dir/pod-lognames)
            do
                    log_pod_name=$(echo $podlog | awk -F_ '{print $1}')
                    log_container_name=$(echo $podlog | awk -F- '{print $NF}' |  awk -F. '{print $1}')
                    # This will help to easily locate the logs by pod name, but also appended the container prefix (5 chars)
                    # to avoid overwriting logs from restarted pods.
                    mv $output_dir/$fluentbit_pod/$log_container_name  $output_dir/$fluentbit_pod/$log_pod_name'-'$(echo $log_container_name | awk '{print substr($0,0,5)}') || true

            done
            # Clean temporal files.
            rm -rf $output_dir/$fluentbit_pod.tar.gz
            rm -rf $output_dir/var
        else
           echo $fluentbit_pod" is not in running state"
        fi
    fi
done

for i in $(cat $output_dir/kubectl-get-po-svc-pvc-all-namespace_output)
do
if [ "`echo $i | awk '{print $1}'`" != "NAMESPACE" ]; then
    # Collect details of resources information (pod, services, persistentvolumeclaim) for all resource phases (e.g. "Running", "Evicted")
    kubectl -n `echo $i | awk '{print $1}'` describe `echo $i | awk '{print $2}'` > $output_dir/describe_`echo $i | awk 'gsub(/\//,"_") {print $2}'`_output
fi
done

cwd=`pwd`
mkdir -p $cwd/pravega-k8s-logs
cd $output_dir
# Create compressed log archive with date stamp
tar zcf $cwd/pravega-k8s-logs/pravega-k8s-logging-`date '+%Y-%m-%d-%H-%M-%S'`.tgz *
rm -rf ../$output_dir
echo "Final logs are available in `ls $cwd/pravega-k8s-logs/*.tgz`"
exit 0
