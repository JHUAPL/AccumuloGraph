AccumuloGraph Table Schema
==========================

AccumuloGraph uses a number of backing tables for data storage.
This file documents the structure and organization of these
tables. The tables and records are structured in such a way as
to implement the Blueprints operations in terms of efficient Accumulo
operations, i.e., prefix searches, contiguous scans, and
batch operations.
For our purposes, Accumulo entries consist of four fields:
row (R), column family (CF), column qualifier (CQ), and value (V).
For more information on Accumulo's schema and how to
access records efficiently, see the
[Accumulo documentation](https://accumulo.apache.org/1.5/accumulo_user_manual.html).

The tables used by AccumuloGraph are prefixed by the configured graph
name and can be classed as element/property tables, and
index tables.  Their structure is discussed below.

Elements and properties
-----------------------

Vertex and edge information, along with their properties, are stored
in the *graphname*\_vertex and *graphname*\_edge tables respectively.

First, an entry declares the existence of a vertex in the vertex table.

| R | CF | CQ | V |
|---|----|----|---|
| *vertex_id* | `_LABEL_` | `_EXISTS_`| *[empty]* |

A similar entry declares the existence of an edge in the edge table.

| R | CF | CQ | V |
|---|----|----|---|
| *edge_id* | `_LABEL_` | *in_vertex_id*`_DELIM_`*out_vertex_id* | *edge_label* |

When adding an edge, additional entries are stored in the
vertex table for each endpoint of the edge. These facilitate the
`Vertex.getEdge` and `Vertex.getVertex` operations.

| R | CF | CQ | V |
|---|----|----|---|
| *in_vertex_id* | `_IN_EDGE_` | *out_vertex_id*`_DELIM_`*edge_id* | *edge_label* |
| *out_vertex_id* | `_OUT_EDGE_` | *in_vertex_id*`_DELIM_`*edge_id* | *edge_label* |

Finally, vertex and edge properties are stored in their respective
tables. Entry formats are the same for both vertices and edges.
Note that property values are serialized such that their type
can be deduced when deserializing.

| R | CF | CQ | V |
|---|----|----|---|
| *element_id* | *property_key* | *[empty]* | *property_value* |

Indexes
-------

Several tables store index-related information,
including index value tables that store index
property keys and values, and index metadata tables
that store information about what indexes exist
and what properties are indexed.

For `KeyIndexableGraph`, index value tables
include *graphname*\_vertex\_key\_index
and *graphname*\_edge\_key\_index for vertex
and edge properties, respectively.
For `IndexableGraph`, index value tables are
named *graphname*\_index\_*indexname*,
where *indexname* is the index name.
The entry formats in all these tables are the same:

| R | CF | CQ | V |
|---|----|----|---|
| *property_value* | *property_key* | *element_id* | *[empty]* |

Property values are serialized in the same way as above.

In addition to the index value tables, an index metadata table,
*graphname*\_index\_metadata, stores index information. For
`KeyIndexableGraph`, records in this table enumerate the property keys
that are indexed.

| R | CF | CQ | V |
|---|----|----|---|
| *property_key* | `_INDEX_KEY_` | *element_class* | *[empty]* |

For `IndexableGraph`, records enumerate the existing indexes.

| R | CF | CQ | V |
|---|----|----|---|
| *index_name* | `_INDEX_NAME_` | *element_class* | *[empty]* |
