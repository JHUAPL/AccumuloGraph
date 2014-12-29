Table Structure
---------------

This file documents the structure of backend tables
used for storing elements, properties, etc.

**Note (29 Dec 2014): This documentation is out of date.**

## Vertex Table

Row ID | Column Family | Column Qualifier | Value
---|---|---|---
VertexID | Label Flag | Exists Flag | [empty]
VertexID | INVERTEX | OutVertexID_EdgeID | Edge Label
VertexID | OUTVERTEX | InVertexID_EdgeID | Edge Label
VertexID | Property Key | [empty] | Serialized Value

## Edge Table

Row ID | Column Family | Column Qualifier | Value
---|---|---|---
EdgeID|Label Flag|InVertexID_OutVertexID|Edge Label
EdgeID|Property Key|[empty]|Serialized Value

## Edge/Vertex Index
Row ID | Column Family | Column Qualifier | Value
---|---|---|---
Serialized Value|Property Key|VertexID/EdgeID|[empty]

## Metadata Table

Row ID | Column Family | Column Qualifier | Value
---|---|---|---
Index Name| Index Class |[empty]|[empty]
