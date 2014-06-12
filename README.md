#AccumuloGraph
=============

**NOTE** Current documentation is a rough and will be finished in near future

## Introduction
 This is an implementation of [TinkerPop Blueprints](http://tinkerpop.com)
 API using [Apache Accumulo](http://apache.accumulo.com) as the back end.
 This implementation is an easy to use, easy to write, and easy to read 
 access to an Arbitrarily Large Graph that is stored in Accumulo.
 
 We implement the following Blueprints interfaces
	<br>1. Graph
	<br>2. KeyIndexableGraph
	<br>3. IndexableGraph
 


##Code Examples
###Creating a new graph
```java
    Configuration cfg = new AccumuloGraphConfiguration()
	    .instance("accumulo").user("user").zkHosts("zk1")
        .password("password".getBytes()).name("myGraph");
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
    Job j = new Job();
    j.setInputFormatClass(VertexInputFormat.class);
    VertexInputFormat.setAccumuloGraphConfiguration(
        new AccumuloGraphConfiguration()
        .instance("accumulo").zkHosts(“zk1").user("root")
        .password("secret".getBytes()).name("myGraph"));
```
####In the mapper
```java
    public void map(Text k, Vertex v, Context c){
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

###Metadate Table
Row ID | Column Family | Column Qualifier | Value
---|---|---|---
Index Name| Index Class |[empty]|[empty]
##Advanced Configuration
###Basic Accumulo Control
###Advance Accumulo Control
###Cacheing
###Preloading


