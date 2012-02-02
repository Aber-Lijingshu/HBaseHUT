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

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

/**
 * Provides utility methods for processing updates in HBase table. 
 */
public final class UpdatesProcessingUtil {
  private UpdatesProcessingUtil() {}

  /**
   * Processes updates in HBase's table.
   * Changes records in HTable by storing update processing results.
   *
   * @param hTable table to process
   * @param updateProcessor update processor
   * @throws IOException when processing fails
   */
  public static void processUpdates(HTable hTable, UpdateProcessor updateProcessor) throws IOException {
    ResultScanner resultScanner =
            new HutResultScanner(hTable.getScanner(new Scan()), updateProcessor, hTable, true);
    while (resultScanner.next() != null) {
      // DO NOTHING
    }
  }

  /**
   * Rolls back all writes performed after or at specified timestamp.
   * NOTE: may not rollback partially in case compaction was performed
   * @param hTable table to perform rollback on
   * @param tsInclusive timestamp
   * @throws IOException when underlying HTable operations throw exception
   */
  // TODO: think about gain of calling HutRowKeyUtil.writtenAfter instead of HutRowKeyUtil.writtenBetween and
  //       may be delegate execution to rollbackWrittenBetween() method
  public static void rollbackWrittenAfter(HTable hTable, final long tsInclusive) throws IOException {
    // todo: shouldn't we create some index to do this faster?
    Scan scan = new Scan(); // all-data scan
    // todo: use hut-specific (fast-forwarding?) server-side filter to fetch only what needs to be deleted
    HTableUtil.ResultFilter filter = new HTableUtil.ResultFilter() {
      @Override
      public boolean accept(Result result) {
        return HutRowKeyUtil.writtenAfter(result.getRow(), tsInclusive);
      }
    };

    HTableUtil.deleteRange(hTable, scan, filter);
  }

  /**
   * Rolls back all writes performed between specified timestamps (inclusively)
   * NOTE: may not rollback partially in case compaction was performed
   * @param hTable table to perform rollback on
   * @param startTsInclusive start timestamp (inclusive)
   * @param stopTsInclusive stop timestamp (inclusive)
   * @throws IOException when underlying HTable operations throw exception
   */
  public static void rollbackWrittenBetween(HTable hTable, final long startTsInclusive, final long stopTsInclusive)
          throws IOException {
    // todo: shouldn't we create some index to do this faster?
    Scan scan = new Scan(); // all-data scan
    // todo: use hut-specific (fast-forwarding?) server-side filter to fetch only what needs to be deleted
    HTableUtil.ResultFilter filter = new HTableUtil.ResultFilter() {
      @Override
      public boolean accept(Result result) {
        return HutRowKeyUtil.writtenBetween(result.getRow(), startTsInclusive, stopTsInclusive);
      }
    };

    HTableUtil.deleteRange(hTable, scan, filter);
  }
}
