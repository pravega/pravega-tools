#### Purpose

Tier-1 is lost completely. The dr tool tries to recover the cluster using the data from Tier-2. 
The main idea is to spin up the SegmentContainer in the tool and make necessary operations reflect in Tier-1 and in Tier-2 then shutdown these container.
After this process when the containers boot up in the production cluster, they will be able to see these persisted changes. 

#### Design
Get the list of the segments with the their segment properties :`isSealed`, `length`.
Group these segments by the container they own.
```
for $containerId = 0 to n-1
    for each $segment in $containerId
        createSegment($segment.name, $segment.isSealed, $segment.length) 
```    
Rename the `_system/containers/metadata_$containerId` to `backup_system/containers/metadata_$containerId` 
```
for old_container_file
    open up the container file using `ContainerTableExtensionImpl.java`
    copy the core attributes for each segment using `updateAttributes` container API 
```    
Start with empty Tier-1. When the container boots up, it creates the necessary metadata tables in memory.

#### Commands

`storage list-segments <root>` - gets the list of segments from Tier-2 and groups them by the container they belong to 

`dr recover <root>`  - updates the segment container metadata and copies the core attributes from the metadata container file stored in Tier-2 