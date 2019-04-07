# Pravega Tools

Tooling for Pravega cluster administration.

## Getting Started

Next, we show how to build the Pravega admin tools.

### Build the tools

Use the built-in gradle wrapper to build the tools.

```
$ ./gradlew build
...
BUILD SUCCESSFUL
```

### Distributing

Use the gradle wrapper to generate the distribution.

```
$ ./gradlew installDist
...

cd build/install/pravega-stream-stat-tool
bin/StreamStat
```