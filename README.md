AccumuloGraph
=============
[![Build Status](https://travis-ci.org/JHUAPL/AccumuloGraph.svg?branch=master)](https://travis-ci.org/JHUAPL/AccumuloGraph)

This is an implementation of the [TinkerPop Blueprints](http://tinkerpop.com)
2.6 API using [Apache Accumulo](http://apache.accumulo.com) as the backend.
This combines the many benefits and flexibility of Blueprints
with the scalability and performance of Accumulo.

In addition to the basic Blueprints functionality, we provide a number
of enhanced features, including:
* Indexing implementations via `IndexableGraph` and `KeyIndexableGraph`
* Support for mock, mini, and distributed instances of Accumulo
* Numerous performance tweaks and configuration parameters
* Support for high speed ingest
* Hadoop integration

Feel free to contact us with bugs, suggestions, pull requests,
or simply how you are leveraging AccumuloGraph in your own work.


## Getting Started

First, include AccumuloGraph as a Maven dependency. Releases are deployed
to Maven Central.

```xml
<dependency>
	<groupId>edu.jhuapl.tinkerpop</groupId>
	<artifactId>blueprints-accumulo-graph</artifactId>
	<version>0.2.1</version>
</dependency>
```

For non-Maven users, the binary jars can be found in the releases section in this
GitHub repository, or you can get them from Maven Central.

Creating an `AccumuloGraph` involves setting a few parameters in an
`AccumuloGraphConfiguration` object, and opening the graph.
The defaults are sensible for using an Accumulo cluster.
We provide some simple examples below. Javadocs for
`AccumuloGraphConfiguration` explain all the other parameters
in more detail.

First, to instantiate an in-memory graph:
```java
Configuration cfg = new AccumuloGraphConfiguration()
  .setInstanceType(InstanceType.Mock)
  .setGraphName("graph");
return GraphFactory.open(cfg);
```

This creates a "Mock" instance which holds the graph in memory.
You can now use all the Blueprints and AccumuloGraph-specific functionality
with this in-memory graph. This is useful for getting familiar
with AccumuloGraph's functionality, or for testing or prototyping
purposes.

To use an actual Accumulo cluster, use the following:
```java
Configuration cfg = new AccumuloGraphConfiguration()
  .setInstanceType(InstanceType.Distributed)
  .setZooKeeperHosts("zookeeper-host")
  .setInstanceName("instance-name")
  .setUser("user").setPassword("password")
  .setGraphName("graph")
  .setCreate(true);
return GraphFactory.open(cfg);
```

This directs AccumuloGraph to use a "Distributed" Accumulo
instance, and sets the appropriate ZooKeeper parameters,
instance name, and authentication information, which correspond
to the usual Accumulo connection settings. The graph name is
used to create several backing tables in Accumulo, and the
`setCreate` option tells AccumuloGraph to create the backing
tables if they don't already exist.

AccumuloGraph also has limited support for a "Mini" instance
of Accumulo.


## Improving Performance

This section describes various configuration parameters that
greatly enhance AccumuloGraph's performance.  Brief descriptions
of each option are provided here, but refer to the
`AccumuloGraphConfiguration` Javadoc for fuller explanations.

### Disable consistency checks

The Blueprints API specifies a number of consistency checks for
various operations, and requires errors if they fail. Some examples
of invalid operations include adding a vertex with the same id as an
existing vertex, adding edges between nonexistent vertices,
and setting properties on nonexistent elements.
Unfortunately, checking the above constraints for an
Accumulo installation entails significant performance issues,
since these require extra traffic to Accumulo using inefficient
non-batched access patterns.

To remedy these performance issues, AccumuloGraph exposes
several options to disable various of the above checks.
These include:
* `setAutoFlush` - to disable automatically flushing
  changes to the backing Accumulo tables
* `setSkipExistenceChecks` - to disable element
  existence checks, avoiding trips to the Accumulo cluster
* `setIndexableGraphDisabled` - to disable
  indexing functionality, which improves performance
  of element removal

### Tweak Accumulo performance parameters

Accumulo itself features a number of performance-related parameters,
and we allow configuration of these. Generally, these relate to
write buffer sizes, multithreading, etc. The settings include:
* `setMaxWriteLatency` - max time prior to flushing
  element write buffer
* `setMaxWriteMemory` - max size for element write buffer
* `setMaxWriteThreads` - max threads used for element writing
* `setMaxWriteTimeout` - max time to wait before failing
  element buffer writes
* `setQueryThreads` - number of query threads to use
  for fetching elements, properties etc.

### Enable edge and property preloading

As a performance tweak, AccumuloGraph performs lazy loading of
properties and edges. This means that an operation such as
`getVertex` does not by default populate the returned
vertex object with the associated vertex's properties
and edges. Instead, they are initialized only when requested via
`getProperty`, `getEdges`, etc.  These are useful
for use cases where you won't be accessing many of these
properties.  However, if certain properties or edges will
be accessed frequently, you can set options for preloading
these specific properties and edges, which will be more
efficient than on-the-fly loading. These options include:
* `setPreloadedProperties` - set property keys
  to be preloaded
* `setPreloadedEdgeLabels` - set edges to be
  preloaded based on their labels

### Enable caching

AccumuloGraph contains a number of caching options
that mitigate the need for Accumulo traffic for recently-accessed
elements. The following options control caching:
* `setVertexCacheParams` - size and expiry for vertex cache
* `setEdgeCacheParams` - size and expiry for edge cache
* `setPropertyCacheTimeout` - property expiry time,
  which can be specified globally and/or for individual properties


## High Speed Ingest

One of Accumulo's key advantages is its ability for high-speed ingest
of huge amounts of data.  To leverage this ability, we provide
an additional `AccumuloBulkIngester` class that
exchanges consistency guarantees for high speed ingest.

The following is an example of how to use the bulk ingester to
ingest a simple graph:
```java
AccumuloGraphConfiguration cfg = ...;
AccumuloBulkIngester ingester = new AccumuloBulkIngester(cfg);
// Add a vertex.
ingester.addVertex("A").finish();
// Add another vertex with properties.
ingester.addVertex("B")
  .add("P1", "V1").add("P2", "V2")
  .finish();
// Add an edge.
ingester.addEdge("A", "B", "edge").finish();
// Shutdown and compact tables.
ingester.shutdown(true);
```

See the Javadocs for more details.
Note that you are responsible for ensuring that data is entered
in a consistent way, or the resulting graph will
have undefined behavior.


## Hadoop Integration

AccumuloGraph features Hadoop integration via custom input and output
format implementations. `VertexInputFormat` and `EdgeInputFormat`
allow vertex and edge inputs to mappers, respectively. Use as follows:
```java
AccumuloGraphConfiguration cfg = ...;

// For vertices:
Job j = new Job();
j.setInputFormatClass(VertexInputFormat.class);
VertexInputFormat.setAccumuloGraphConfiguration(j, cfg);

// For edges:
Job j = new Job();
j.setInputFormatClass(EdgeInputFormat.class);
EdgeInputFormat.setAccumuloGraphConfiguration(j, cfg);
```

`ElementOutputFormat` allows writing to an AccumuloGraph from
reducers. Use as follows:
```java
AccumuloGraphConfiguration cfg = ...;

Job j = new Job();
j.setOutputFormatClass(ElementOutputFormat.class);
ElementOutputFormat.setAccumuloGraphConfiguration(j, cfg);
```

## Rexster Configuration
Below is a snippet to show an example of AccumuloGraph integration with Rexster. For a complete list of options for configuration, see [`AccumuloGraphConfiguration$Keys`](https://github.com/JHUAPL/AccumuloGraph/blob/master/src/main/java/edu/jhuapl/tinkerpop/AccumuloGraphConfiguration.java#L110) 

```xml
<graph>
	<graph-enabled>true</graph-enabled>
	<graph-name>myGraph</graph-name>
	<graph-type>edu.jhuapl.tinkerpop.AccumuloRexsterGraphConfiguration</graph-type>
	<properties>
		<blueprints.accumulo.instance.type>Distributed</blueprints.accumulo.instance.type>
		<blueprints.accumulo.instance>accumulo</blueprints.accumulo.instance>
		<blueprints.accumulo.zkhosts>zk1,zk2,zk3</blueprints.accumulo.zkhosts>
		<blueprints.accumulo.user>user</blueprints.accumulo.user>
		<blueprints.accumulo.password>password</blueprints.accumulo.password>
	</properties>
	<extensions>
	</extensions>
</graph>
```
