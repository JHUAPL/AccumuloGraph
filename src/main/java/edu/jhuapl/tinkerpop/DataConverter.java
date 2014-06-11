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

public class DataConverter {

	public static Value toValue(Object data) {
		if (data instanceof String) {
			String val = "S" + data;
			return new Value(val.getBytes());
		} else if (data instanceof Value) {
			return (Value) data;
		}

		throw new UnsupportedOperationException("TODO: handle "
				+ data.getClass());
	}

	@SuppressWarnings("unchecked")
	public static <T> T fromValue(Value value) {
		byte[] data = value.get();
		switch (data[0]) {
		case 'S':
			return (T) new String(data, 1, data.length - 1);
		}
		throw new UnsupportedOperationException("TODO: handle type: " + data[0]);
	}
}
