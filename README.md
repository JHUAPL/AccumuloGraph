AccumuloGraph
=============
[![Build Status](https://travis-ci.org/JHUAPL/AccumuloGraph.svg?branch=master)](https://travis-ci.org/JHUAPL/AccumuloGraph)

This is an implementation of the [TinkerPop Blueprints](http://tinkerpop.com)
2.6 API using [Apache Accumulo](http://apache.accumulo.com) as the backend.
This implementation provides easy to use, easy to write, and easy to read 
access to an arbitrarily large graph that is stored in Accumulo.
 
We implement the following Blueprints interfaces:
	<br>1. Graph
	<br>2. KeyIndexableGraph
	<br>3. IndexableGraph
	
Please feel free to submit issues for any bugs you find or features you want.
We are open to pull requests from your forks also.

##Usage

The releases are currently stored in Maven Central.
```xml
<dependency>
	<groupId>edu.jhuapl.tinkerpop</groupId>
	<artifactId>blueprints-accumulo-graph</artifactId>
	<version>0.0.2</version>
</dependency>
```

For non-Maven users, the binaries can be found in the releases section in this
GitHub repository, or you can get them from Maven Central.
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

