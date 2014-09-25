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

public class AccumuloGraphException extends RuntimeException {

  private static final long serialVersionUID = 2179662107592532517L;

  public AccumuloGraphException() {

  }

  public AccumuloGraphException(String reason) {
    super(reason);
  }

  public AccumuloGraphException(Throwable cause) {
    super(cause);
  }

  public AccumuloGraphException(String reason, Throwable cause) {
    super(reason, cause);
  }
}
