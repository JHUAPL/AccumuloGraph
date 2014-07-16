AccumuloGraph
=============
[![Build Status](https://travis-ci.org/JHUAPL/AccumuloGraph.svg?branch=master)](https://travis-ci.org/JHUAPL/AccumuloGraph)

This is an implementation of the [TinkerPop Blueprints](http://tinkerpop.com)
API using [Apache Accumulo](http://apache.accumulo.com) as the backend.
This implementation provides easy to use, easy to write, and easy to read 
access to an arbitrarily large graph that is stored in Accumulo.
 
We implement the following Blueprints interfaces:
	<br>1. Graph
	<br>2. KeyIndexableGraph
	<br>3. IndexableGraph

##Code Examples
###Creating a new distributed graph
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
###Basic Accumulo Control
TODO
###Advanced Accumulo Control
TODO
###Caching
TODO
###Preloading
TODO
