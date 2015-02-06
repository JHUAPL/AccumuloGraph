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

| R | CF | CQ | V |
|---|----|----|---|
| *vertex_id* | `_LABEL_` | `_EXISTS_`| *[empty]* |

| R | CF | CQ | V |
|---|----|----|---|
| *edge_id* | `_LABEL_` | *in_vertex_id*`_DELIM_`*out_vertex_id* | *edge_label* |

| R | CF | CQ | V |
|---|----|----|---|
| *in_vertex_id* | `_IN_EDGE_` | *out_vertex_id*`_DELIM_`*edge_id* | *edge_label* |
| *out_vertex_id* | `_OUT_EDGE_` | *in_vertex_id*`_DELIM_`*edge_id* | *edge_label* |

| R | CF | CQ | V |
|---|----|----|---|
| *element_id* | *property_key* | *[empty]* | *property_value* |

Indexes
-------

Relevant table names:

1. *graphname*\_vertex\_key\_index - Vertex property key index
2. *graphname*\_edge\_key\_index - Edge property key index
3. *graphname*\_indexed\_keys - List of indexed keys
4. *graphname*\_index\_names - List of named index names
5. *graphname*\_index\_*indexname* - Named index

| R | CF | CQ | V |
|---|----|----|---|
| *property_value* | *property_key* | *element_id* | *[empty]* |

| R | CF | CQ | V |
|---|----|----|---|
| *property_key* | *element_class* | *[empty]* | *[empty]* |

| R | CF | CQ | V |
|---|----|----|---|
| *index_name* | *element_class* | *[empty]* | *[empty]* |
