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
package com.sematext.hbase.hut.up;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.sematext.hbase.hut.DebugUtil;
import com.sematext.hbase.hut.HutPut;
import com.sematext.hbase.hut.HutResultScanner;
import com.sematext.hbase.hut.TestHBaseHut;
import com.sematext.hbase.hut.UpdateProcessor;

public class TestSumColumnsUp {
  public static final byte[] CF = Bytes.toBytes("cf");
  public static final byte[] VALUE1_C = Bytes.toBytes("v1");
  public static final byte[] VALUE2_C = Bytes.toBytes("v2");
  public static final byte[] VALUE3_C = Bytes.toBytes("v3");
  private static final String TABLE_NAME = "table";
  private HBaseTestingUtility testingUtility;
  private HTable hTable;

  @Before
  public void before() throws Exception {
    testingUtility = new HBaseTestingUtility();
    testingUtility.startMiniZKCluster();
    testingUtility.startMiniCluster(1);
    hTable = testingUtility.createTable(Bytes.toBytes(TABLE_NAME), CF);
  }

  @After
  public void after() throws IOException {
    hTable.close();
    hTable = null;
    testingUtility.shutdownMiniCluster();
    testingUtility.shutdownMiniZKCluster();
    testingUtility = null;
  }

  @Test
  public void testHutScan() throws IOException, InterruptedException {
    SumColumnsUp up = new SumColumnsUp(
            new Pair<byte[], byte[]>(CF, VALUE1_C),
            new Pair<byte[], byte[]>(CF, VALUE2_C),
            new Pair<byte[], byte[]>(CF, VALUE3_C));

    byte[] source1 = Bytes.toBytes("source1");
    writeMeasurement(source1, 55L, 12L, null);
    writeMeasurement(source1, null, 5L, 7L);

    byte[] source2 = Bytes.toBytes("source2");
    writeMeasurement(source2, 13L, 11L, null);
    writeMeasurement(source2, null, 3L, null);

    byte[] source3 = Bytes.toBytes("source3");
    writeMeasurement(source3, 27L, null, null);

    System.out.println(DebugUtil.getContentAsText(hTable));

    verifyMeasurementWithHutScan(up, source1, 55L, 17L, 7L);
    verifyMeasurementWithHutScan(up, source2, 13L, 14L, null);
    verifyMeasurementWithHutScan(up, source3, 27L, null, null);
  }

  @Test
  public void testMr() throws IOException, InterruptedException, ClassNotFoundException {
    SumColumnsUp up = new SumColumnsUp(
            new Pair<byte[], byte[]>(CF, VALUE1_C),
            new Pair<byte[], byte[]>(CF, VALUE2_C),
            new Pair<byte[], byte[]>(CF, VALUE3_C));

    byte[] source1 = Bytes.toBytes("source1");
    writeMeasurement(source1, 55L, 12L, null);
    writeMeasurement(source1, null, 5L, 7L);

    byte[] source2 = Bytes.toBytes("source2");
    writeMeasurement(source2, 13L, 11L, null);
    writeMeasurement(source2, null, 3L, null);

    byte[] source3 = Bytes.toBytes("source3");
    writeMeasurement(source3, 27L, null, null);

    TestHBaseHut.processUpdatesWithMrJob(testingUtility.getConfiguration(), hTable,
                                         100, 32 * 1024, 0, up);

    verifyMeasurementWithNativeScan(up, source1, 55L, 17L, 7L);
    verifyMeasurementWithNativeScan(up, source2, 13L, 14L, null);
    verifyMeasurementWithNativeScan(up, source3, 27L, null, null);
  }

  private void writeMeasurement(byte[] sourceId, Long val1, Long val2, Long val3) throws IOException {
    Put put = new Put(HutPut.adjustRow(sourceId));
    if (val1 != null) {
      put.add(CF, VALUE1_C, Bytes.toBytes(val1));
    }
    if (val2 != null) {
      put.add(CF, VALUE2_C, Bytes.toBytes(val2));
    }
    if (val3 != null) {
      put.add(CF, VALUE3_C, Bytes.toBytes(val3));
    }
    hTable.put(put);
  }

  private void verifyMeasurementWithNativeScan(UpdateProcessor up, byte[] sourceId, Long val1, Long val2, Long val3) throws IOException {
    ResultScanner scanner = hTable.getScanner(getScan(sourceId));
    verify(scanner, val1, val2, val3);
  }

  private void verifyMeasurementWithHutScan(UpdateProcessor up, byte[] sourceId, Long val1, Long val2, Long val3) throws IOException {
    ResultScanner scanner = hTable.getScanner(getScan(sourceId));
    ResultScanner resultScanner = new HutResultScanner(scanner, up);
    verify(resultScanner, val1, val2, val3);
  }

  private void verify(ResultScanner resultScanner, Long val1, Long val2, Long val3) {
    int count = 0;
    for (Result res : resultScanner) {
      count++;

      verify(res, VALUE1_C, val1);
      verify(res, VALUE2_C, val2);
      verify(res, VALUE3_C, val3);
    }

    Assert.assertEquals(1, count);
  }

  private void verify(Result res, byte[] qual, Long val) {
    byte[] v = res.getValue(CF, qual);
    if (val == null) {
      Assert.assertNull(v);
      return;
    }

    Assert.assertEquals((long) val, Bytes.toLong(v));
  }

  private static Scan getScan(byte[] rowKey) {
    byte[] stopRow = Arrays.copyOf(rowKey, rowKey.length);
    stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
    return new Scan(rowKey, stopRow);
  }
}
