/**
 * Copyright 2010 Sematext International
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
package com.sematext.hbase.hut;

import java.util.Arrays;

/**
 * Byte array wrapper. It can be used as a HashMap key.
 * It is assumed that array is never changes. (Yep, we could copy array in constructor
 * to enforce that, we just don't want to use redundant space)
 * TODO: isn't there some existing class for that?
 */
public class ByteArrayWrapper {
  private final byte[] array;
  private final int hash;

  public ByteArrayWrapper(byte[] array) {
    this.array = array;
    this.hash = array != null ? Arrays.hashCode(array) : 0;
  }

  public byte[] getBytes() {
    return array;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ByteArrayWrapper that = (ByteArrayWrapper) o;

    return Arrays.equals(array, that.array);
  }

  @Override
  public int hashCode() {
    return hash;
  }
}
