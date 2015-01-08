/* Copyright 2014 The Johns Hopkins University Applied Physics Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
