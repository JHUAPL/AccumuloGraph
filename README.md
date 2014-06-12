#AccumuloGraph
=============

*NOTE* Current documentation is a rough and will be finished in near future

## Introduction
 This is an implementation of [TinkerPop Blueprints](http://tinkerpop.com) API using [Apache Accumulo](apache.accumulo.com) as the back end.
 
 We implement the following Blueprints interfaces
	<br>1. [KeyIndexableGraph]()
	<br>2. [IndexableGraph]()
 


##Code Examples
###Creating a new graph

    Configuration cfg = new AccumuloGraphConfiguration()
	    .instance("accumulo").user("user").zkHosts("zk1")	.password("password".getBytes()).name("myGraph");
    Graph graph = GraphFactory.open(cfg);


###Accessing a graph

    Vertex v1 = graph.addVertex("1");
    v1.setProperty("name", "Alice");
    Vertex v2 = graph.addVertex("2");
    v2.setProperty("name", "Bob");

    Edge e1 = graph.addEdge("E1", v1, v2, "knows");
    e1.setProperty("since", new Date());



###Creating indexes

    ((KeyIndexableGraph)graph)
	    .createKeyIndex("name", Vertex.class);

###MapReduce Integration

####In the tool

    Job j = new Job();
    j.setInputFormatClass(VertexInputFormat.class);
    VertexInputFormat.setAccumuloGraphConfiguration(
        new AccumuloGraphConfiguration()
        .instance(“accumulo").zkHosts(“zk1").user("root")
        .password(“secret".getBytes()).name("myGraph"));
	
####In the mapper

    public void map(Text k, Vertex v, Context c){
        System.out.println(v.getId().toString());
    }

