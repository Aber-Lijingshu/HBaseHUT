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

import java.io.IOException;

/**
 * Provides utility methods for HBaseHUT operations on HTable.
 * These operations cannot be easily integrated within current HBase API (pre Coprocessors release).
 * NOTE: This class made as static util and not as wrapper for ease of use of HTable instances when HTablePool is used 
 */
public final class HutUtil {
  private HutUtil() {}

  /**
   * Performs deletion.
   * The same as {@link org.apache.hadoop.hbase.client.HTable#delete(Delete)} operation, i.e.
   * <tt>hTable.delete(delete)</tt> should be substituted to <tt>HutUtil.delete(hTable, delete)</tt>
   *
   * @param hTable table to perform deletion in
   * @param delete record to delete
   * @throws java.io.IOException when underlying HTable operations throw exception
   */
  public static void delete(HTable hTable, Delete delete) throws IOException {
    delete(hTable, delete.getRow());
  }

  /**
   * Performs deletion.
   * See also {@link #delete(org.apache.hadoop.hbase.client.HTable, org.apache.hadoop.hbase.client.Delete)}.
   * @param hTable table to perform deletion in
   * @param row row of record to delete
   * @throws java.io.IOException when underlying HTable operations throw exception
   */
  public static void delete(HTable hTable, byte[] row) throws IOException {
    byte[] stopRow = HutRowKeyUtil.createNewKey(row, Long.MAX_VALUE);
    HTableUtil.deleteRange(hTable, row, stopRow);
  }
}
