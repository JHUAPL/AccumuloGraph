AccumuloGraph
=============
[![Build Status](https://travis-ci.org/JHUAPL/AccumuloGraph.svg?branch=master)](https://travis-ci.org/JHUAPL/AccumuloGraph)

This is an implementation of the [TinkerPop Blueprints](http://tinkerpop.com)
2.6 API using [Apache Accumulo](http://apache.accumulo.com) as the backend.
This combines the benefits and flexibility of Blueprints
with the scalability and performance of Accumulo.

In addition to the basic Blueprints functionality, we provide additional
features that harness more of Accumulo's power.

Some features include...


Benchmarks


Indexing via the `IndexableGraph` and `KeyIndexableGraph` interfaces.

Benchmarking

Feel free to email with suggestions for improvements.
Please submit issues for any bugs you find or features you want.
We are also open to pull requests.


This implementation provides easy to use, easy to write, and easy to read 
access to an arbitrarily large graph that is stored in Accumulo.
 
We implement the following Blueprints interfaces:
	<br>1. Graph
	<br>2. KeyIndexableGraph
	<br>3. IndexableGraph
	
Benchmarking.



## Getting Started

First, include AccumuloGraph as a Maven dependency. Releases are deployed
to Maven Central.

```xml
<dependency>
	<groupId>edu.jhuapl.tinkerpop</groupId>
	<artifactId>blueprints-accumulo-graph</artifactId>
	<version>0.0.2</version>
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

### Set Accumulo performance parameters

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

### Caching and preloading

AccumuloGraph contains a number of 

* `setPropertyCacheTimeout`

* `setEdgeCacheParams`
* `setVertexCacheParams`

* `setPreloadedEdgeLabels`
* `setPreloadedProperties`


## Bulk Ingest



## Hadoop Integration


## Table Structure



##Code Examples
###Creating a new or connecting to an existing distributed graph
```java
Configuration cfg = new AccumuloGraphConfiguration()
	.setInstanceName("accumulo").setUser("user").setZookeeperHosts("zk1")
    .setPassword("password".getBytes()).setGraphName("myGraph");
Graph graph = GraphFactory.open(cfg.getConfiguration());
```
###Creating a new Mock Graph

Setting the instance type to mock allows for in-memory processing with a MockAccumulo instance.<br>
There is also support for Mini Accumulo.
```java
Configuration cfg = new AccumuloGraphConfiguration().setInstanceType(InstanceType.Mock)
	.setGraphName("myGraph");
Graph graph = GraphFactory.open(cfg);
```
###Accessing a graph
```java
Vertex v1 = graph.addVertex("1");
v1.setProperty("name", "Alice");
Vertex v2 = graph.addVertex("2");
v2.setProperty("name", "Bob");

Edge e1 = graph.addEdge("E1", v1, v2, "knows");
e1.setProperty("since", new Date());
 ```


###Creating indexes

```java
((KeyIndexableGraph)graph)
	.createKeyIndex("name", Vertex.class);
```
###MapReduce Integration

####In the tool
```java
AccumuloConfiguration cfg = new AccumuloGraphConfiguration()
	.setInstanceName("accumulo").setZookeeperHosts("zk1").setUser("root")
	.setPassword("secret".getBytes()).setGraphName("myGraph");

Job j = new Job();
j.setInputFormatClass(VertexInputFormat.class);
VertexInputFormat.setAccumuloGraphConfiguration(j,
	cfg.getConfiguration());
```
####In the mapper
```java
public void map(Text k, Vertex v, Context c) {
    System.out.println(v.getId().toString());
}
 ``` 

##Table Design
###Vertex Table
Row ID | Column Family | Column Qualifier | Value
---|---|---|---
VertexID | Label Flag | Exists Flag | [empty]
VertexID | INVERTEX | OutVertexID_EdgeID | Edge Label
VertexID | OUTVERTEX | InVertexID_EdgeID | Edge Label
VertexID | Property Key | [empty] | Serialized Value
###Edge Table
Row ID | Column Family | Column Qualifier | Value
---|---|---|---
EdgeID|Label Flag|InVertexID_OutVertexID|Edge Label
EdgeID|Property Key|[empty]|Serialized Value
###Edge/Vertex Index
Row ID | Column Family | Column Qualifier | Value
---|---|---|---
Serialized Value|Property Key|VertexID/EdgeID|[empty]

###Metadata Table
Row ID | Column Family | Column Qualifier | Value
---|---|---|---
Index Name| Index Class |[empty]|[empty]
##Advanced Configuration
###Graph Configuration
- setGraphName(String name)
- setCreate(boolean create) - Sets if the backing graph tables should be created if they do not exist.
- setClear(boolean clear) - Sets if the backing graph tables should be reset if they exist.
- autoFlush(boolean autoFlush) - Sets if each graph element and property change will be flushed to the server.
- skipExistenceChecks(boolean skip) - Sets if you want to skip existance checks when creating graph elemenets.
- setAutoIndex(boolean ison) - Turns on/off automatic indexing.

###Accumulo Control

- setUser(String user) - Sets the user to use when connecting to Accumulo
- setPassword(byte[] password | String password) - Sets the password to use when connecting to Accumulo
- setZookeeperHosts(String zookeeperHosts) - Sets the Zookeepers to connect to.
- setInstanceName(String instance) - Sets the Instance name to use when connecting to Zookeeper
- setInstanceType(InstanceType type) - Sets the type of Instance to use : Distrubuted, Mini, or Mock. Defaults to Distrubuted
- setQueryThreads(int threads) - Specifies the number of threads to use in scanners. Defaults to 3
- setMaxWriteLatency(long latency) - Sets the latency to be used for all writes to Accumulo
- setMaxWriteTimeout(long timeout) - Sets the timeout to be used for all writes to Accumulo
- setMaxWriteMemory(long mem) - Sets the memory buffer to be used for all writes to Accumulo
- setMaxWriteThreads(int threads) - Sets the number of threads to be used for all writes to Accumulo
- setAuthorizations(Authorizations auths) - Sets the authorizations to use when accessing the graph
- setColumnVisibility(ColumnVisibility colVis) - TODO
- setSplits(String splits | String[] splits) - Sets the splits to use when creating tables. Can be a space sperated list or an array of splits 
- setMiniClusterTempDir(String dir) - Sets directory to use as the temp directory for the Mini cluster

###Caching
- setLruMaxCapacity(int max) - TODO
- setVertexCacheTimeout(int millis) - Sets the vertex cache timeout.  A value <=0 clears the value
- setEdgeCacheTimeout(int millis)  - Sets the edge cache timeout.  A value <=0 clears the value

###Preloading
- setPropertyCacheTimeout(int millis) - Sets the element property cache timeout. A value <=0 clears the value
- setPreloadedProperties(String[] propertyKeys) - Sets the property keys that should be preloaded. Requiers a positive timout.
- setPreloadedEdgeLabels(String[] edgeLabels) - TODO

