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

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/**
 */
public final class DebugUtil {
  private static final String C_DEL = "\t\t";
  private static final String CF_DEL = ":";
  private static final String VAL_DEL = "=";

  public DebugUtil() {}

  public static String getContentAsText(HTable t) throws IOException {
    ResultScanner rs = t.getScanner(new Scan());
    Result next = rs.next();
    int readCount = 0;
    StringBuilder contentText = new StringBuilder();
    while (next != null && readCount < 1000) {
      append(contentText, next);
      contentText.append("\n");
      next = rs.next();
      readCount++;
    }

    return contentText.toString();
  }

  public static void append(StringBuilder contentText, Result next) {
    contentText.append(getHutRowKeyAsText(next.getRow()));
    for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cf : next.getMap().entrySet()) {
      for (Map.Entry<byte[], NavigableMap<Long, byte[]>> c : cf.getValue().entrySet()) {
        byte[] value = c.getValue().values().iterator().next();
        contentText.append(C_DEL);
        contentText.append(Bytes.toString(cf.getKey())).append(CF_DEL)
                .append(Bytes.toString(c.getKey())).append(VAL_DEL)
                .append(getText(value));
      }
    }
  }

  public static String getAsText(Result next) {
    StringBuilder text = new StringBuilder();
    append(text, next);

    return text.toString();
  }

  private static String getHutRowKeyAsText(byte[] row) {
    return Bytes.toString(HutRowKeyUtil.getOriginalKey(row)) + "-" +
            Bytes.toLong(Bytes.head(Bytes.tail(row, 2 * Bytes.SIZEOF_LONG), Bytes.SIZEOF_LONG)) + "-" +
            Bytes.toLong(Bytes.tail(row, Bytes.SIZEOF_LONG));
  }

  private static String getText(byte[] data) {
    try {
      if (data.length == Bytes.SIZEOF_INT) {
        return String.valueOf(Bytes.toInt(data));
      } else if (data.length == Bytes.SIZEOF_LONG) {
        return String.valueOf(Bytes.toLong(data));
      } else {
        return Bytes.toString(data);
      }
    } catch (IllegalArgumentException ex) {
      return Bytes.toString(data);
    }
  }

  public static void clean(HTable t) throws IOException {
    // TODO: do HBaseAdmin.deleteTable() instead?
    ResultScanner rs = t.getScanner(new Scan());
    Result next = rs.next();
    while (next != null) {
      t.delete(new Delete(next.getRow())); // TODO: delete in batches?
      next = rs.next();
    }
  }

}
