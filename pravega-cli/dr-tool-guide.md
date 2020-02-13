#### Purpose

The following document describes the recovery procedure to be followed in case of losing Tier-1 completely. The `dr recover <root>` command tries to recover the cluster using the data from Tier-2. 
The main idea is to spin up the SegmentContainers in the CLI and make the necessary operations such creating segments, updating attributes reflect in Tier-1 and Tier-2 and then shutdown these containers.
After this process when the containers boot up in the production cluster, they will be able to see these persisted changes. 

### Prerequites
1. Service Pod should have a volume mount for Tier-2
2. Service Pod should have a volume mount for Tier-1
3. The BK and ZK services must have been restored prior to starting the recovery procedure.

#### Design

In CLI,
1. Rename container metadata files in Tier-2. (_system/containers/metadata_$<containerId>)
2. Spin up the segment containers
3. Expose an API to create segment with the predefined state. Invoke this API for each of the entries in the output of `storage list-segments <root>`.
4. For each container metadata files (i.e. old container metadata) from step# 1, copy truncate offset and core attributes for any segments created in step #3

#### Implementation
(1) Get the list of the segments with the their segment properties :`isSealed`, `length`.

(2) Group these segments by the container they own into say `segToContainer_$containerId` file.

(3) 
```
for $containerId = 0 to n-1
    open the container
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

#### Commands

`storage list-segments <root>` - gets the list of segments from Tier-2 and groups them by the container they belong to (generates one file per container). 

`dr recover <root>`  - updates the segment container metadata and copies the core attributes from the metadata container file stored in Tier-2. 