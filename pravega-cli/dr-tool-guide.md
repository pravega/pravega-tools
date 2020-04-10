## Purpose
The following document summarizes the recovery procedure to be followed in the following disaster scenarios. 
In all the scenarios below, it is assumed that Tier-2 data is stable and accessible.

1. Lost Tier-1 data completely.\
 (a) Complete loss of BK data and complete loss of ZK data.\
 (b) Complete loss of BK data and No loss of ZK data.
2. Lost Tier-1 data partially.\
 (a) Partial loss of BK data and complete loss of ZK data.\
 (b) Partial loss of BK data and partial loss of ZK data.
3. Lost only ZK data.

### Scenario #1a: Tier-1 data is lost completely
#### Prerequites
1. Service Pod should have a volume mount for Tier-2
2. The BK and ZK services must have been restored and be available prior to starting the recovery procedure below.

#### Running Recovery Command
1. Follow https://github.com/pravega/pravega-tools/tree/master/pravega-cli to get the pravega-cli executable.
2. Run `dr recover <root>` 
This command will spin up the SegmentContainers in the CLI and makes the necessary operations such as creating segments, updating attributes etc to reflect in Tier-1 and Tier-2 and then shutdowns these containers.
After this process when the containers boot up in the production cluster, they will be able to see these persisted changes. 

#### Design
In CLI,
1. Invoke `storage list-segments` API to get the segments, from Tier 2, grouped by the container they belong to.
2. Spin up the segment containers in recover mode. In this mode, certain APIs are made accessible for the CLI. 
At present, only `createSegments` API with pre-defined state is exposed. 
3. After each container boots up, iterate over the segments owned by that container to do the following:
    1. If a segment is present in both metadata and Tier 2, fix its length.
    2. If a segment is only present in Tier 2, create it with the length obtained from Tier 2
    3. If a segment is only present in metadata, delete it.
4. All the user segments are sealed to simplify the recovery i.e. attributes for inactive segments will be read from the storage. 

#### Testing

1. Restore BK and ZK with an empty Tier-1
2. Shutdown segment stores
3. Run the recovery command `dr recover <root>`
4. Bring back the segment stores
5. Verify if the data can be read correctly.