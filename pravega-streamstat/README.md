# pravega-streamstat
A Pravega Stream Status Tool

## Usage
```
./gradlew installDist
```

Distributions will be installed in 
```
build/install/pravega-stream-status-tool
```

use
```
bin/StreamStat
```
to run program.

## Instructions
```
usage: StreamStat
 -a,--all        Display all of the container logs.
 -c,--cluster    Display the cluster information.
 -d,--data       Display all the data in the stream. (By default, only
                 print data length)
 -e,--explicit   Wait until get the explicit log, since the last log might
                 be locked by the pravega cluster
 -h,--help       Print this help.
 -i,--input      Input stream name to get status. (You can also set stream
                 name in config file stream="[scope]/[stream]")
 -l,--log        Display the logs related to given stream in tier-1.
 -r,--recover    Recover mode, if zookeeper's metadata is inconsistent with 
                 storage, recreate the metadata.
 -s,--storage    Give the offset to start with.
 -t,--txn        Print information about all the transactions.
 ```


## Features

This tool will display the stream segments and their data's length and offset range in tier-1 and tier-2 by default.

The default stream is `examples/someStream`
The two inputs are zookeeper url and hdfs url.
You can edit the configure file in `conf/config.properties`
Or copy the pravega cluster configure file to this location.

| **Options** | **Feature**                                                 | 
|-------------|-------------------------------------------------------------|
| **-a**      | Display all the logs in container (not only the logs related to the given stream), should be used with -l.  |
| **-c**      | Display the information of the pravega cluster.             |
| **-d**      | In both tier, print out the data stored. Otherwise program will only print datalength.|
| **-e**      | Since some log might be locked by the pravega cluster, it may take some time to wait until the cluster release the log, program won't wait by default, but to get the explicit log, you should use this option to wait until the explicit log is read.|
| **-h**      | Display help message.           |
| **-i**      | Input the stream you want to inspect. You can also set stream in config.properties |
| **-r**      | Recover mode, if zookeeper's metadata is inconsistent with storage, recreate the metadata.|
| **-l**      | Display the logs related to given stream. |
| **-s**      | Give the offset to start with. |
| **-t**      | Display all the transactions related to given stream. |