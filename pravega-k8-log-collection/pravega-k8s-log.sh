#!/bin/bash
mkdir -p /tmp/logdir
output_dir="/tmp/logdir"
kubectl get po,svc,pvc --all-namespaces > $output_dir/kubectl-get-po-svc-pvc-all-namespace_output
IFS=$'\n'
for i in $(cat $output_dir/kubectl-get-po-svc-pvc-all-namespace_output | grep -i pravega)
do
echo $i
if [[ $i == *"po"* ]];
then 
    if [ `echo $i | awk '{print $5}'` != 0 ]
        then     
            kubectl -n `echo $i | awk '{print $1}'` logs `echo $i | awk '{print $2}'` -p > $output_dir/log_`echo $i | awk 'gsub(/\//,"_") {print $2}'`_previous.log
    fi
    kubectl -n `echo $i | awk '{print $1}'` logs `echo $i | awk '{print $2}'` > $output_dir/log_`echo $i | awk 'gsub(/\//,"_") {print $2}'`_recent.log
    kubectl -n `echo $i | awk '{print $1}'` exec -it `echo $i |  awk '-F'[/] '{print $2}'|awk '{print $1}'` -- ls /opt/pravega/logs > /dev/null 2>&1
    if [ $? == 0 ]; 
        then
            mkdir -p $output_dir/oldlogs_`echo $i |  awk '-F'[/] '{print $2}'|awk '{print $1}'` 
            kubectl -n `echo $i | awk '{print $1}'` cp `echo $i |  awk '-F'[/] '{print $2}'|awk '{print $1}'`:/opt/pravega/logs $output_dir/oldlogs_`echo $i |  awk '-F'[/] '{print $2}'|awk '{print $1}'`/.
    fi    
fi
kubectl -n `echo $i | awk '{print $1}'` describe `echo $i | awk '{print $2}'` > $output_dir/describe_`echo $i | awk 'gsub(/\//,"_") {print $2}'`_output
done
cwd=`pwd`
mkdir -p $cwd/pravega-k8s-logs
cd $output_dir
tar zcf $cwd/pravega-k8s-logs/pravega-k8s-logging-`date '+%Y-%m-%d-%H-%M-%S'`.tgz *
rm -rf $output_dir
echo "Final logs are avaialble in `ls $cwd/pravega-k8s-logs/*.tgz`"
