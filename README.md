# pravega-tools

Tooling for Pravega

## Getting Started

### Build Pravega

Optional: This step is required only if you want to use a different version of Pravega than is published to maven central.

Install the Pravega client libraries to your local Maven repository:

```
$ git clone https://github.com/pravega/pravega.git
$./gradlew install
```

### Build the tools

Use the built-in gradle wrapper to build the tools.

```
$ ./gradlew build
...
BUILD SUCCESSFUL
```

### Distributing

Use the gradle wrapper to generat the distribution.

```
$ ./gradlew installDist
...

cd build/install/pravega-stream-stat-tool
bin/StreamStat
```