AccumuloGraph
=============
Creating a new graph
...
Configuration cfg = new AccumuloGraphConfiguration()
	.instance("accumulo").user("user").zkHosts("zk1")	.password("password".getBytes()).name("myGraph");
Graph graph = GraphFactory.open(cfg);
...

Accessing a graph
...
Vertex v1 = graph.addVertex("1");
v1.setProperty("name", "Alice");
Vertex v2 = graph.addVertex("2");
v2.setProperty("name", "Bob");

Edge e1 = graph.addEdge("E1", v1, v2, "knows");
e1.setProperty("since", new Date());
...


Creating indexes
...
((KeyIndexableGraph)graph)
	.createKeyIndex("name", Vertex.class);
...

