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

import com.tinkerpop.blueprints.Features;

/**
 * {@link Features} creator.
 */
public class AccumuloFeatures {

  public static Features get() {
    Features f = new Features();

    // For simplicity, I accept all property types. They are handled in not the
    // best way. To be fixed later.
    f.ignoresSuppliedIds = true;
    f.isPersistent = true;
    f.isWrapper = false;
    f.supportsBooleanProperty = true;
    f.supportsDoubleProperty = true;
    f.supportsDuplicateEdges = true;
    f.supportsEdgeIndex = true;
    f.supportsEdgeIteration = true;
    f.supportsEdgeRetrieval = true;
    f.supportsEdgeKeyIndex = true;
    f.supportsEdgeProperties = true;
    f.supportsFloatProperty = true;
    f.supportsIndices = true;
    f.supportsIntegerProperty = true;
    f.supportsKeyIndices = true;
    f.supportsLongProperty = true;
    f.supportsMapProperty = true;
    f.supportsMixedListProperty = true;
    f.supportsPrimitiveArrayProperty = true;
    f.supportsSelfLoops = true;
    f.supportsSerializableObjectProperty = true;
    f.supportsStringProperty = true;
    f.supportsThreadedTransactions = false;
    f.supportsTransactions = false;
    f.supportsUniformListProperty = true;
    f.supportsVertexIndex = true;
    f.supportsVertexIteration = true;
    f.supportsVertexKeyIndex = true;
    f.supportsVertexProperties = true;
    f.supportsThreadIsolatedTransactions = false;

    return f;
  }
}
