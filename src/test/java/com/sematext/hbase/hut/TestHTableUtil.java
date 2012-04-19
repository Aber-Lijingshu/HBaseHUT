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

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class TestHTableUtil {
  @Test
  public void testConvert() throws Exception {
    HBaseTestingUtility testingUtility = new HBaseTestingUtility();
    testingUtility.startMiniCluster();
    HTable hTable = testingUtility.createTable(Bytes.toBytes("mytable"),
                                               new byte[][] {Bytes.toBytes("colfam1"), Bytes.toBytes("colfam2")});

    byte[] key = Bytes.toBytes("some-key");
    Put put = new Put(key);
    put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1.1"), System.currentTimeMillis(), Bytes.toBytes("val1.1"));
    put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1.2"), System.currentTimeMillis(), Bytes.toBytes("val1.2"));
    put.add(Bytes.toBytes("colfam2"), Bytes.toBytes("qual2.1"), System.currentTimeMillis(), Bytes.toBytes("val2.1"));

    Result converted = HTableUtil.convert(put);

    hTable.put(put);

    Result readFromTable = hTable.get(new Get(key));

    Assert.assertArrayEquals(readFromTable.raw(), converted.raw());

    testingUtility.shutdownMiniCluster();
  }
}
