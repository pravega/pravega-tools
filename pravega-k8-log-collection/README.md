# pravega-k8-log-collection
Pravega K8 based deployment log collection tool

The shell script (`pravega-k8s-log.sh`) which will do below stuff:
1. list all pod, services and persistent volumes running in the K8 deployment
2. Check all Pravega components associated
3. If component is a pod it will collect log from that container:
	 It will collect recent logs
	 It will check whether container had been restarted due to any reason and would take logs from previous deployment as well (if any).
	 It will take complete logs (including logrotated so far) from Pravega Segmentstore and Controller (e.g. /opt/pravega/logs)  
4. Take details of all pod, service and pvcs (describe output)
5. Create a tarball with time stamp in present working directory under `pravega-k8s-logs`

Sample Usage:

`# chmod +x pravega-k8s-log.sh`

`# ./pravega-k8s-log.sh`

Logs collected in $PWD/pravega-k8s-logs

Sample log output:
/home/work/pravega-k8-log-collection/pravega-k8s-logs/pravega-k8s-logging-2018-12-05-07-06-26.tgz

Note:
1. Script assumes valid kubernetes configuration present in the node where it is invoked.
2. `kubectl` should be present in the system.
3. Doesn't handle logs from `evicted` pods and collect only describe output for those.
