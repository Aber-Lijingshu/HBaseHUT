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
import java.util.ArrayList;

/**
 * Provides utility methods on top of {@link org.apache.hadoop.hbase.client.HTable}
 */
final class HTableUtil {
  private static final int DELETES_BUFFER_MAX_SIZE = 10000; // TODO: Make configurable by user ?

  private HTableUtil() {}

  public static void deleteRange(HTable hTable, byte[] firstInclusive, byte[] lastInclusive, byte[] processingResultToLeave) throws IOException {
    Scan deleteScan = getDeleteScan(firstInclusive, lastInclusive);
    ResultScanner toDeleteScanner = hTable.getScanner(deleteScan);
    Result toDelete = toDeleteScanner.next();
    // Huge number of deletes can eat up all memory, hence keeping buffer
    ArrayList<Delete> listToDelete = new ArrayList<Delete>(DELETES_BUFFER_MAX_SIZE);
    while (toDelete != null) {
      if (!Bytes.equals(processingResultToLeave, toDelete.getRow())) {
        listToDelete.add(new Delete(toDelete.getRow()));
        if (listToDelete.size() >= DELETES_BUFFER_MAX_SIZE) {
          hTable.delete(listToDelete);
          listToDelete.clear();
        }
      }
      toDelete = toDeleteScanner.next();
    }
    // it is omitted during scan, since stopRow specified for scan means "non-inclusive", so adding here
    if (!Bytes.equals(processingResultToLeave, lastInclusive)) {
      listToDelete.add(new Delete(lastInclusive));
    }

    if (listToDelete.size() > 0) {
      hTable.delete(listToDelete);
    }
  }

  public static void deleteRange(HTable hTable, byte[] firstInclusive, byte[] lastNonInclusive) throws IOException {
    Scan deleteScan = getDeleteScan(firstInclusive, lastNonInclusive);
    ResultScanner toDeleteScanner = hTable.getScanner(deleteScan);
    Result toDelete = toDeleteScanner.next();
    // Huge number of deletes can eat up all memory, hence keeping buffer
    ArrayList<Delete> listToDelete = new ArrayList<Delete>(DELETES_BUFFER_MAX_SIZE);
    while (toDelete != null) {
      listToDelete.add(new Delete(toDelete.getRow()));
      if (listToDelete.size() >= DELETES_BUFFER_MAX_SIZE) {
        hTable.delete(listToDelete);
        listToDelete.clear();
      }
      toDelete = toDeleteScanner.next();
    }

    if (listToDelete.size() > 0) {
      hTable.delete(listToDelete);
    }
  }

  private static Scan getDeleteScan(byte[] firstInclusive, byte[] lastNonInclusive) {
    // TODO: think over limiting of fetched data: set single columnFam:qual with small value to fetch
    return new Scan(firstInclusive, lastNonInclusive);
  }
}
