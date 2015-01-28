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

/**
 * Collect up various constants here.
 * @author Michael Lieberman
 *
 */
public class Constants {

  private Constants() { }

  public static final String ID_DELIM = "__DELIM__";

  public static final byte[] EMPTY = new byte[0];

  /**
   * Prefixes for various Accumulo entries.
   */
  public static final String LABEL = "__LABEL__";
  public static final String IN_EDGE = "__INEDGE__";
  public static final String OUT_EDGE = "__OUTEDGE__";
  public static final String EXISTS = "__EXISTS__";
}
