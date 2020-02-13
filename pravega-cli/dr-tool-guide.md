## Purpose
The following document summarizes the recovery procedure to be followed in the following disaster scenarios. 
In all the scenarios below, it is assumed that Tier-2 data is accessible and intact.
1. Lost Tier-1 data completely. This includes the data stored by bookies and the data stored by ZooKeeper.
2. Lost Tier-1 data partially. 
 (a) Partial data loss of bookies and the complete loss of ZooKeeper data.
 (b) Partial data loss of bookies and the partial loss of ZooKeeper data.
3. Lost only ZooKeeper Tier-1 data.

### Scenario #1: Tier-1 data is lost completely
#### Prerequites
1. Service Pod should have a volume mount for Tier-2
2. Service Pod should have a volume mount for Tier-1
3. The BK and ZK services must have been restored prior to starting the recovery procedure.

#### Commands
`dr recover <root>`   
This command will spin up the SegmentContainers in the CLI and make the necessary operations such as creating segments, updating attributes etc to reflect in Tier-1 and Tier-2 and then shutdowns these containers.
After this process when the containers boot up in the production cluster, they will be able to see these persisted changes. 

#### Design

In CLI,
1. Rename container metadata files in Tier-2. (_system/containers/metadata_$<containerId>)
2. Spin up the segment containers
3. Use the createSegment API to create segment with the predefined state. Invoke this API for user segments in `storage list-segments <root>` output.
4. For each container metadata files (i.e. old container metadata) from step# 1, copy truncate offset and core attributes for any segments created in step #3

#### Implementation
(1) Get the list of the segments with the their segment properties :`isSealed`, `length`.

(2) Group these segments by the container they own into say `segToContainer_$containerId` file.

(3) 
```
for $containerId = 0 to n-1
    spin up the container for $containerId
    for each $segment in `segToContainer_$containerId` file
        createSegment($segment.name, $segment.isSealed, $segment.length) 
```
(4) Rename the  `_system/containers/metadata_$containerId` to `backup_system/containers/metadata_$containerId`

(5) 
```
for each old_container_file
      open up the container file using `ContainerTableExtensionImpl.java`
      copy the core attributes for each segment using `updateAttributes` container API 
```
#### Testing

1. Simulate Tier-1 loss by removing `/pravega/<scope>/segmentstore` using `zkCli.sh`.
2. Run the `dr recover <root>` cmd.
3. (TODO) controller recovery procedure
4. Restart Pravega segment store pods.
5. (TODO) Verify all the data from Tier-2 can be read.