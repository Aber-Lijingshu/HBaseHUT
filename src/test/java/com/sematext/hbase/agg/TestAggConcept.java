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
package com.sematext.hbase.agg;

import java.io.IOException;

import com.sematext.hbase.hut.HutPut;
import com.sematext.hbase.hut.HutResultScanner;
import com.sematext.hbase.hut.HutRowKeyUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * General unit-test for the standard aggregation logic concept.
 * NOTE: this by no means represents the final standard aggregation API or implementation. This class was added
 * as a stub for further development.
 */
public class TestAggConcept {
  private static final byte[] VALUE_QUAL = Bytes.toBytes("value");
  private HBaseTestingUtility testingUtility;
  public static final byte[] METRICS_CF = Bytes.toBytes("m");

  @Before
  public void before() throws Exception {
    testingUtility = new HBaseTestingUtility();
    testingUtility.startMiniCluster();
  }

  @After
  public void after() throws IOException {
    testingUtility.shutdownMiniCluster();
    testingUtility = null;
  }

  @Test
  public void testSimpleScan() throws IOException, InterruptedException {
    HTable hTable = testingUtility.createTable(Bytes.toBytes("stock-market"), METRICS_CF);

    double[] maxValues = new double[3];
    for (int i = 0; i < 100; i++) {
      double metricValue = Math.random();
      int metricIndex = i % 3;
      String metricId = "metric" + metricIndex; // thus, we have 3 metrics
      if (maxValues[metricIndex] < metricValue) {
        maxValues[metricIndex] = metricValue;
      }
      saveMetricValue(hTable, metricId, metricValue);
    }

    MaxFunction maxFunction = new MaxFunction(METRICS_CF, VALUE_QUAL);
    ResultScanner resultScanner =
            new HutResultScanner(hTable.getScanner(new Scan()), maxFunction);
    int comparedCount = 0;
    int count = 0;
    for (Result result : resultScanner) {
      count++;
      for (int i = 0; i < maxValues.length; i++) {
        if (("metric" + i).equals(Bytes.toString(HutRowKeyUtil.getOriginalKey(result.getRow())))) {
          comparedCount++;
          Assert.assertEquals(maxValues[i], Bytes.toDouble(result.getValue(METRICS_CF, VALUE_QUAL)), 0.001);
        }
      }
    }

    Assert.assertEquals(3, count);
    Assert.assertEquals(3, comparedCount);

    hTable.close();
  }

  private static void saveMetricValue(HTable hTable, String metricId, double value) throws InterruptedException, IOException {
    Put put = new HutPut(Bytes.toBytes(metricId));
    put.add(METRICS_CF, VALUE_QUAL, Bytes.toBytes(value));
    Thread.sleep(1); // sanity interval
    hTable.put(put);

  }
}
