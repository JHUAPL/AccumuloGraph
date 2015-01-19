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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;

import javax.xml.namespace.QName;

public final class AccumuloByteSerializer {

  public static final int NULL = 'n';

  public static final int BYTE = 'b';
  public static final int SHORT = 's';
  public static final int CHARACTER = 'c';
  public static final int INTEGER = 'i';
  public static final int LONG = 'l';
  public static final int FLOAT = 'f';
  public static final int DOUBLE = 'd';
  public static final int BOOLEAN = 'o';
  public static final int DATE = 't';
  public static final int ENUM = 'e';
  public static final int STRING = 'a';
  public static final int SERIALIZABLE = 'x';
  public static final int QNAME = 'q';

  private AccumuloByteSerializer() {

  }

  private static final ThreadLocal<ByteArrayOutputStream> BOUTS = new ThreadLocal<ByteArrayOutputStream>() {
    @Override
    public ByteArrayOutputStream initialValue() {
      return new ByteArrayOutputStream(32);
    }
  };

  @SuppressWarnings("unchecked")
  public static <T> T deserialize(byte[] target) {
    if (target[0] == NULL) {
      return null;
    }

    switch (target[0]) {
      case BYTE:
        return (T) (Byte) Byte.parseByte(new String(target, 1, target.length - 1));
      case SHORT:
        return (T) (Short) Short.parseShort(new String(target, 1, target.length - 1));
      case CHARACTER:
        return (T) (Character) new String(target, 1, target.length - 1).charAt(0);
      case INTEGER:
        return (T) (Integer) Integer.parseInt(new String(target, 1, target.length - 1));
      case LONG:
        return (T) (Long) Long.parseLong(new String(target, 1, target.length - 1));
      case FLOAT:
        return (T) (Float) Float.parseFloat(new String(target, 1, target.length - 1));
      case DOUBLE:
        return (T) (Double) Double.parseDouble(new String(target, 1, target.length - 1));
      case BOOLEAN:
        switch (new String(target, 1, 1)) {
          case "t":
            return (T) Boolean.TRUE;
          case "f":
            return (T) Boolean.FALSE;
          default:
            throw new RuntimeException("Unexpected boolean value: " + target[1]);
        }
      case DATE:
        long millis = Long.parseLong(new String(target, 1, target.length - 1));
        return (T) new Date(millis);
      case STRING:
        return (T) new String(target, 1, target.length - 1);
      case QNAME:
        return (T) QName.valueOf(new String(target, 1, target.length - 1));
      case ENUM:
        try {
          String[] s = new String(target, 1, target.length - 1).split(":");
          Class<? extends Enum> clz = (Class<? extends Enum>) Class.forName(s[0]);
          return (T) Enum.valueOf(clz, s[1]);
        } catch (ClassNotFoundException cnfe) {
          throw new RuntimeException("Unexpected error deserializing object.", cnfe);
        }
      case SERIALIZABLE:
        try {
          ByteArrayInputStream bin = new ByteArrayInputStream(target, 1, target.length);
          ObjectInputStream ois = new ObjectInputStream(bin);
          return (T) ois.readObject();
        } catch (IOException io) {
          throw new RuntimeException("Unexpected error deserializing object.", io);
        } catch (ClassNotFoundException cnfe) {
          throw new RuntimeException("Unexpected error deserializing object.", cnfe);
        }
      case NULL:
    	return null;
      default:
        throw new RuntimeException("Unexpected data type: " + (char) target[0]);
    }
  }

  public static byte[] serialize(Object o) {
    ByteArrayOutputStream bout = BOUTS.get();

    try {
      if (o == null) {
        bout.write(NULL);
        return bout.toByteArray();
      }

      String val = o.toString();
      int type = -1;
      String cls = o.getClass().getSimpleName();
      switch (cls) {
        case "Byte":
          type = BYTE;
          break;
        case "Short":
          type = SHORT;
          break;
        case "Character":
          type = CHARACTER;
          break;
        case "Integer":
          type = INTEGER;
          break;
        case "Long":
          type = LONG;
          break;
        case "Float":
          type = FLOAT;
          break;
        case "Double":
          type = DOUBLE;
          break;
        case "Boolean":
          type = BOOLEAN;
          if ((Boolean) o) {
            val = "t";
          } else {
            val = "f";
          }
          break;
        case "Date":
          val = Long.toString(((Date) o).getTime());
          type = DATE;
          break;
        case "String":
          type = STRING;
          break;
        case "QName":
          type = QNAME;
          break;
        default:
          if (o instanceof Enum) {
            type = ENUM;
            val = o.getClass().getName() + ":" + val;
          } else if (o instanceof Serializable) {
            bout.write(SERIALIZABLE);
            ObjectOutputStream oos = new ObjectOutputStream(bout);
            oos.writeObject(o);
            oos.close();
            return bout.toByteArray();
          } else {
            throw new RuntimeException("Unsupported data type: " + o.getClass());
          }
      }

      bout.write(type);
      bout.write(val.getBytes());
      return bout.toByteArray();
    } catch (IOException io) {
      throw new RuntimeException("Unexpected error writing to byte array.", io);
    } finally {
      bout.reset();
    }
  }
}
