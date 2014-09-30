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
package edu.jhuapl.tinkerpop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloGraph;

public abstract class MapReduceElement implements Element, WritableComparable<MapReduceElement> {

  protected String id;

  protected Map<String,Object> properties;

  protected Map<String,Object> newProperties;

  AccumuloGraph parent;

  MapReduceElement(AccumuloGraph parent) {
    this.parent = parent;
    properties = new HashMap<String,Object>();
    newProperties = new HashMap<String,Object>();
  }

  void prepareId(String id) {
    this.id = id;
  }

  void prepareProperty(String key, Object property) {
    properties.put(key, property);
  }

  Map<String,Object> getNewProperties() {
    return newProperties;
  }

  @Override
  public Object getId() {
    return id;
  }

  @Override
  public <T> T getProperty(String key) {

    Object newProp = newProperties.get(key);
    if (newProp != null)
      return (T) newProp;
    return (T) properties.get(key);
  }

  @Override
  public Set<String> getPropertyKeys() {
    return Sets.union(new HashSet<String>(properties.keySet()), new HashSet<String>(newProperties.keySet()));
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("You cannot remove an element during a MapReduce job.");
  }

  @Override
  public <T> T removeProperty(String key) {
    throw new UnsupportedOperationException("You cannot modify an element during a MapReduce job.");
  }

  @Override
  public void setProperty(String key, Object value) {
    newProperties.put(key, value);
  }

  protected Graph getParent() {
    return parent;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    prepareId(in.readUTF());

    properties.clear();
    int count = in.readInt();
    for (int i = 0; i < count; i++) {
      String key = in.readUTF();
      byte[] data = new byte[in.readInt()];
      in.readFully(data);
      Object val = AccumuloByteSerializer.desserialize(data);
      properties.put(key, val);
    }

    count = in.readInt();
    for (int i = 0; i < count; i++) {
      String key = in.readUTF();
      byte[] data = new byte[in.readInt()];
      in.readFully(data);
      Object val = AccumuloByteSerializer.desserialize(data);
      newProperties.put(key, val);
    }

  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(id);
    out.writeInt(properties.size());
    for (String key : properties.keySet()) {
      out.writeUTF(key);
      byte[] data = AccumuloByteSerializer.serialize(properties.get(key));
      out.writeInt(data.length);
      out.write(data);
    }

    for (String key : newProperties.keySet()) {
      out.writeUTF(key);
      byte[] data = AccumuloByteSerializer.serialize(newProperties.get(key));
      out.writeInt(data.length);
      out.write(data);
    }
  }

  @Override
  public int compareTo(MapReduceElement other) {
    int val = this.id.compareTo(other.id);
    if (val != 0) {
      return val;
    }
    return getClass().getSimpleName().compareTo(other.getClass().getSimpleName());
  }

  public boolean equals(Object object) {
    if (object == this) {
      return true;
    } else if (object == null) {
      return false;
    } else if (!object.getClass().equals(getClass())) {
      return false;
    } else {
      Element element = (Element) object;
      if (id == null) {
        return element.getId() == null;
      } else {
        return id.equals(element.getId());
      }
    }
  }

  public int hashCode() {
    int hash = 31 * getClass().hashCode();
    if (id != null) {
      hash ^= id.hashCode();
    }
    return hash;
  }

}
