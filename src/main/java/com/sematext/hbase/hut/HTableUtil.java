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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides utility methods on top of {@link org.apache.hadoop.hbase.client.HTable}
 */
final class HTableUtil {
  private static final int DELETES_BUFFER_MAX_SIZE = 10000; // TODO: Make configurable by user ?

  private HTableUtil() {}

  private static Delete createHutDelete(byte[] rowKey) {
    Delete delete = new Delete(rowKey);
    // Increases performance a lot. In case of failure this may cause junk records to remain, but
    // will not corrupt the data: these records will be skipped while reading
    // TODO: implement sanitary "junk cleaner" (MR?) job to fight with consequences.
    delete.setWriteToWAL(false);
    return delete;
  }

  public static void deleteRange(HTable hTable, byte[] firstInclusive, byte[] lastNonInclusive) throws IOException {
    Scan deleteScan = getDeleteScan(firstInclusive, lastNonInclusive);
    deleteRange(hTable, deleteScan);
  }

  public static interface ResultFilter {
    boolean accept(Result result);
  }

  public static void deleteRange(HTable hTable, Scan deleteScan) throws IOException {
    deleteRange(hTable, deleteScan, null);
  }

  public static void deleteRange(HTable hTable, Scan deleteScan, ResultFilter resultFilter) throws IOException {
    ResultScanner toDeleteScanner = hTable.getScanner(deleteScan);
    Result toDelete = toDeleteScanner.next();
    // Huge number of deletes can eat up all memory, hence keeping buffer
    // TODO: implement patch for HTable to support buffering deletes instead
    ArrayList<Delete> listToDelete = new ArrayList<Delete>(DELETES_BUFFER_MAX_SIZE);
    while (toDelete != null) {
      if (resultFilter == null || resultFilter.accept(toDelete)) {
        listToDelete.add(createHutDelete(toDelete.getRow()));
        if (listToDelete.size() >= DELETES_BUFFER_MAX_SIZE) {
          hTable.delete(listToDelete);
          listToDelete.clear();
        }
      }
      toDelete = toDeleteScanner.next();
    }

    if (listToDelete.size() > 0) {
      hTable.delete(listToDelete);
    }
  }

  public static Result convert(Put put) {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    for (List<KeyValue> l : put.getFamilyMap().values()) {
      kvs.addAll(l);
    }
    Result result = new Result(kvs);

    return result;
  }

  private static Scan getDeleteScan(byte[] firstInclusive, byte[] lastNonInclusive) {
    // TODO: think over limiting of fetched data: set single columnFam:qual with small value to fetch
    Scan scan = new Scan(firstInclusive, lastNonInclusive);
    // TODO: make configurable?
    scan.setCaching(1024);

    scan.setFilter(new FirstKeyOnlyFilter());

    return scan;
  }
}
