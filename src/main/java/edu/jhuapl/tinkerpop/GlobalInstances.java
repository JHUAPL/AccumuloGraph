/******************************************************************************
 *                              COPYRIGHT NOTICE                              *
 * Copyright (c) 2014 The Johns Hopkins University/Applied Physics Laboratory *
 *                            All rights reserved.                            *
 *                                                                            *
 * This material may only be used, modified, or reproduced by or for the      *
 * U.S. Government pursuant to the license rights granted under FAR clause    *
 * 52.227-14 or DFARS clauses 252.227-7013/7014.                              *
 *                                                                            *
 * For any other permissions, please contact the Legal Office at JHU/APL.     *
 ******************************************************************************/
package edu.jhuapl.tinkerpop;

import org.apache.accumulo.core.client.MultiTableBatchWriter;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.tables.EdgeTableWrapper;
import edu.jhuapl.tinkerpop.tables.ElementTableWrapper;
import edu.jhuapl.tinkerpop.tables.VertexTableWrapper;

/**
 * Internal class gathering together instances of
 * objects needed for various AccumuloGraph components.
 */
public class GlobalInstances {

  private final AccumuloGraph graph;
  private final AccumuloGraphConfiguration config;
  private final MultiTableBatchWriter mtbw;
  private VertexTableWrapper vertexWrapper;
  private EdgeTableWrapper edgeWrapper;
  private final ElementCaches caches;

  public GlobalInstances(AccumuloGraph graph,
      AccumuloGraphConfiguration config, MultiTableBatchWriter mtbw,
      ElementCaches caches) {
    this.graph = graph;
    this.config = config;
    this.mtbw = mtbw;
    this.caches = caches;
  }

  public AccumuloGraph getGraph() {
    return graph;
  }

  public AccumuloGraphConfiguration getConfig() {
    return config;
  }

  public MultiTableBatchWriter getMtbw() {
    return mtbw;
  }

  public VertexTableWrapper getVertexWrapper() {
    return vertexWrapper;
  }

  public EdgeTableWrapper getEdgeWrapper() {
    return edgeWrapper;
  }

  public <T extends Element> ElementTableWrapper getElementWrapper(Class<T> clazz) {
    if (Vertex.class.equals(clazz)) {
      return vertexWrapper;
    } else if (Edge.class.equals(clazz)) {
      return edgeWrapper;
    } else {
      throw new AccumuloGraphException("Unrecognized class: "+clazz);
    }
  }

  public ElementCaches getCaches() {
    return caches;
  }

  /**
   * TODO: Refactor these away when the {@link #graph} member is gone.
   * @param vertexWrapper
   */
  @Deprecated
  public void setVertexWrapper(VertexTableWrapper vertexWrapper) {
    this.vertexWrapper = vertexWrapper;
  }

  /**
   * TODO: Refactor these away when the {@link #graph} member is gone.
   * @param vertexWrapper
   */
  @Deprecated
  public void setEdgeWrapper(EdgeTableWrapper edgeWrapper) {
    this.edgeWrapper = edgeWrapper;
  }
}
