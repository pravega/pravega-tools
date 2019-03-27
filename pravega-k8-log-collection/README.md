# Pravega Log Collection Utilities for Kubernetes

We present the available log collection scripts for Pravega deployed on a Kubernetes cluster.

## Collect logs from Pravega services

The shell script (`pravega-k8s-log.sh`) which will do below stuff:
1. List all pods, services and persistent volumes running in the K8 deployment.
2. Check all Pravega components associated.
3. If component is a pod it will collect log from that container: i) It will collect recent logs.
ii) It will check whether container had been restarted due to any reason and would take logs from previous deployment as well (if any).
iii) It will take complete logs (including logrotated so far) from Pravega Segmentstore and Controller (e.g. `/opt/pravega/logs`).

4. Take details of all pod, service and pvcs (describe output).
5. Create a tarball with time stamp in present working directory under `pravega-k8s-logs`.

Sample usage:

`# chmod +x pravega-k8s-log.sh`

`# ./pravega-k8s-log.sh`

Logs collected in `$PWD/pravega-k8s-logs`

Sample log output:

`/home/work/pravega-k8-log-collection/pravega-k8s-logs/pravega-k8s-logging-2018-12-05-07-06-26.tgz`

Note:
1. Script assumes valid kubernetes configuration present in the node where it is invoked.
2. `kubectl` should be present in the system.
3. Does not handle logs from `evicted` pods and collect only describe output for those.

## Collect logs from Fluentbit service

The shell script (`pravega-k8s-fluentbit-log.sh`) does the following:
1. List all pods, services and persistent volumes running in the K8 deployment.
2. Check all Fluentbit pods in the current deployment.
3. For each Fluentbit pod: i) Creates a temporary `tar.gz` file with all the logs. 
ii) Downloads the compressed log file and tries to structure the folders in a meaningful way for developers inspecting the logs.
4. Take details of all pod, service and pvcs (describe output).
5. Create a tarball with time stamp in present working directory under `pravega-k8s-logs`.

Sample usage:

`# chmod +x pravega-k8s-fluentbit-log.sh`

`# ./pravega-k8s-fluentbit-log.sh`

Logs collected in `$PWD/pravega-k8s-logs`

Sample log output:

`/home/work/pravega-k8-log-collection/pravega-k8s-logs/pravega-k8s-logging-2019-03-01-08-15-36.tgz`

Note:
1. Script assumes valid kubernetes configuration present in the node where it is invoked.
2. `kubectl` should be present in the system.
3. Script assumes that there is a Fluentbit service storing data locally (local storage or remote volume) via the syslog output plugin.
