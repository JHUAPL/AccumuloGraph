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
package edu.jhuapl.tinkerpop.mutator.property;

import com.tinkerpop.blueprints.Element;

import edu.jhuapl.tinkerpop.mutator.BaseMutator;

public abstract class BasePropertyMutator extends BaseMutator {

  protected Element element;
  protected String key;

  public BasePropertyMutator(Element element, String key) {
    this.element = element;
    this.key = key;
  }
}
