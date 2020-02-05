#### Purpose

Tier-1 is lost completely. The dr tool tries to recover the cluster using the data from Tier-2. 
The main idea is to spin up the SegmentContainer in the tool and make necessary operations reflect in Tier-1 and in Tier-2 then shutdown these container.
After this process when the containers boot up in the production cluster, they will be able to see these persisted changes. 

### Prerequites
1. Service Pod should have a volume mount for Tier-2
2. Service Pod should have a volume mount for Tier-1

#### Design
In CLI,
1. Rename container metadata files in Tier-2. (_system/containers/metadata_$<containerId>)
2. Boot up segment containers
3. Create Segments with the predefined state (from `storage list-segments <root>`) using an API exposed from `DebugStreamSegmentContainer` class
4. For each metadata files from step# 1, copy truncate offset and core attributes for any segments created in step #3

#### Implementation
1. Get the list of the segments with the their segment properties :`isSealed`, `length`.
2. Group these segments by the container they own.
3.
```
for $containerId = 0 to n-1
    for each $segment in $containerId
        createSegment($segment.name, $segment.isSealed, $segment.length) 
```
4.Rename the  `_system/containers/metadata_$containerId` to `backup_system/containers/metadata_$containerId`
5. 
```
for old_container_file
      open up the container file using `ContainerTableExtensionImpl.java`
      copy the core attributes for each segment using `updateAttributes` container API 
```

#### Commands

`storage list-segments <root>` - gets the list of segments from Tier-2 and groups them by the container they belong to 

`dr recover <root>`  - updates the segment container metadata and copies the core attributes from the metadata container file stored in Tier-2 