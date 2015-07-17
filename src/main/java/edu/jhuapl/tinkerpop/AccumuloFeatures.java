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

import java.util.UUID;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.FeatureSet;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;

/**
 * {@link Features} creator.
 */
public class AccumuloFeatures implements Features {
  public GraphFeatures graph() {
    return new AccumuloGraphFeatures();
  }


  public VertexFeatures vertex() {
    return new AccumuloVertexFeatures() {};
  }

  public EdgeFeatures edge() {
    return new AccumuloEdgeFeatures() {};
  }
}

class AccumuloGraphFeatures implements GraphFeatures {
  @FeatureDescriptor(name = FEATURE_COMPUTER)
  public boolean supportsComputer() {
    return false;
  }

  @FeatureDescriptor(name = FEATURE_PERSISTENCE)
  public boolean supportsPersistence() {
    return false;
  }

  @FeatureDescriptor(name = FEATURE_TRANSACTIONS)
  public boolean supportsTransactions() {
    return false;
  }

  @FeatureDescriptor(name = FEATURE_THREADED_TRANSACTIONS)
  public boolean supportsThreadedTransactions() {
    return false;
  }

  public VariableFeatures variables() {
    return new VariableFeatures(){};
  }

}

class AccumuloEdgeFeatures extends AccumuloElementFeatures implements EdgeFeatures {

  public  EdgePropertyFeatures properties() {
      return new EdgePropertyFeatures() {
      };
  }
}

class AccumuloElementFeatures implements ElementFeatures {

}
class AccumuloVertexFeatures extends AccumuloElementFeatures implements VertexFeatures{
  public VertexProperty.Cardinality getCardinality(final String key) {
    return VertexProperty.Cardinality.single;
  }


  @FeatureDescriptor(name = FEATURE_MULTI_PROPERTIES)
  public boolean supportsMultiProperties() {
    return false;
  }

  @FeatureDescriptor(name = FEATURE_META_PROPERTIES)
  public boolean supportsMetaProperties() {
    return false;
  }

  public VertexPropertyFeatures properties() {
    return new AccumuloVertexPropertyFeatures();
  }
}
class AccumuloVertexPropertyFeatures implements VertexPropertyFeatures {

}
//
// public static Features get() {
// Features f = new Features();
//
// // For simplicity, I accept all property types. They are handled in not the
// // best way. To be fixed later.
// f.ignoresSuppliedIds = true;
// f.isPersistent = true;
// f.isWrapper = false;
// f.supportsBooleanProperty = true;
// f.supportsDoubleProperty = true;
// f.supportsDuplicateEdges = true;
// f.supportsEdgeIndex = true;
// f.supportsEdgeIteration = true;
// f.supportsEdgeRetrieval = true;
// f.supportsEdgeKeyIndex = true;
// f.supportsEdgeProperties = true;
// f.supportsFloatProperty = true;
// f.supportsIndices = true;
// f.supportsIntegerProperty = true;
// f.supportsKeyIndices = true;
// f.supportsLongProperty = true;
// f.supportsMapProperty = true;
// f.supportsMixedListProperty = true;
// f.supportsPrimitiveArrayProperty = true;
// f.supportsSelfLoops = true;
// f.supportsSerializableObjectProperty = true;
// f.supportsStringProperty = true;
// f.supportsThreadedTransactions = false;
// f.supportsTransactions = false;
// f.supportsUniformListProperty = true;
// f.supportsVertexIndex = true;
// f.supportsVertexIteration = true;
// f.supportsVertexKeyIndex = true;
// f.supportsVertexProperties = true;
// f.supportsThreadIsolatedTransactions = false;
//
// return f;
// }
// }
