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

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.util.StringFactory;

final class AccumuloGraphUtils {

	public static final Value EMPTY_VALUE = new Value(new byte[0]);
	public static final Text EMPTY_TEXT = new Text("");

	public static final Text ID = new Text(StringFactory.ID);

	public static final Text IN = new Text("I");
	public static final Text OUT = new Text("O");
	
	public static final String toId(Object obj) {
		return obj.toString();
	}
	
	public static Value toValue(String val) {
		return new Value(val.getBytes());
	}

}
