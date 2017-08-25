# pravega-streamstat

A Pravega Stream Audit Tool

## Pre requisites

1. Java 8
2. Runing pravega cluster

## Publish Pravega jars to local Maven (optional)

If you have downloaded a nightly build of Pravega, you may need to generate the latest Pravega jar files and publish them to your local Maven repository. For release builds of Pravega, the artifacts will already be in Maven Central and you won't need to run this step.

Note: maven 2 needs to be installed and running on your machine

In the root of Pravega (where Pravega's build.gradle file can be found), run:

`
$ ./gradlew install
`

The above command should generate the required jar files into your local maven repo.

## Generate scripts for you to run the program with configurations.
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

## Options
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

For pravega developers and pravega users:

Use this tool with a running pravega cluster to inspect the data inside pravega storage. It will display the stream segments and their data's length and offset range in tier-1 and tier-2 by default.

#### Schematic

The program will run three printers serially, 

* `io.pravega.tools.pravegastreamstat.zookeeper.ZKFormatPrintImpl`
* `io.pravega.tools.pravegastreamstat.logs.LogFormatPrintImpl`
* `io.pravega.tools.pravegastreamstat.storage.StorageFormatPrint`

Which will print the data stored in each part. And also use a `MetadataCollector` to collect the metadata stored in each part.

#### Senarios

* When an unxpected read happens, find out what's happening inside the pravega storage.
* Find out the status of a transaction on a given stream. If it is still active or during the procedure of aborting.
* Find out if a cluster is still healthy.
* Inspect the log stored in tier-1, inspect what has happened to a segment.

#### Instruction

The default stream is `examples/someStream`.

Put the pravega's segmentstore configuration file in `conf/config.properties`.


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