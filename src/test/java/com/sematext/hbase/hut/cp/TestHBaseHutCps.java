/**
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
package com.sematext.hbase.hut.cp;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.sematext.hbase.hut.DebugUtil;
import com.sematext.hbase.hut.HutPut;
import com.sematext.hbase.hut.TestHBaseHut;
import com.sematext.hbase.hut.UpdateProcessor;

/**
 * High-level unit-test for the idea of utilizing CPs for fetching data wirtten with HBaseHut
 */
public class TestHBaseHutCps {
  public static final byte[] SALE_CF = Bytes.toBytes("sale");
  private static final String TABLE_NAME = "stock-market";
  private static final byte[] CHRYSLER = Bytes.toBytes("chrysler");
  private static final byte[] FORD = Bytes.toBytes("ford");
  private static final byte[] TOYOTA = Bytes.toBytes("toyota");
  private HBaseTestingUtility testingUtility = new HBaseTestingUtility();
  private HTable hTable;

  @Before
  public void before() throws Exception {
    testingUtility.getConfiguration().setStrings(
            CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
            HutReadEndpoint.class.getName());
    testingUtility.startMiniCluster();
    hTable = testingUtility.createTable(Bytes.toBytes(TABLE_NAME), SALE_CF);
  }

  @After
  public void after() throws Exception {
    hTable = null;
    testingUtility.shutdownMiniCluster();
    testingUtility = null;
  }

  @Test
  public void testCoprocessorsStuff() throws Throwable {
    final TestHBaseHut.StockSaleUpdateProcessor processor = new TestHBaseHut.StockSaleUpdateProcessor();

    // Writing data
    verifyLastSalesForAllCompanies(hTable, processor, new int[][]{{}, {}});
    recordSale(hTable, CHRYSLER, 90);
    recordSale(hTable, CHRYSLER, 100);
    recordSale(hTable, FORD, 18);
    recordSale(hTable, CHRYSLER, 120);
    recordSale(hTable, TOYOTA, 202);
    recordSale(hTable, CHRYSLER, 115);
    recordSale(hTable, TOYOTA, 212);
    recordSale(hTable, TOYOTA, 204);
    recordSale(hTable, FORD, 22);
    recordSale(hTable, CHRYSLER, 110);
    recordSale(hTable, CHRYSLER, 105);
    recordSale(hTable, FORD, 24);
    recordSale(hTable, FORD, 28);
    hTable.flushCommits();

    System.out.println(DebugUtil.getContentAsText(hTable));
    verifyLastSalesForAllCompanies(hTable, processor, new int[][]{{105, 110, 115, 120, 100}, {28, 24, 22, 18}, {204, 212, 202}});
    verifyLastSales(hTable, processor, TOYOTA, new int[] {204, 212, 202});
    verifyLastSales(hTable, processor, CHRYSLER, new int[] {105, 110, 115, 120, 100});
    verifyLastSales(hTable, processor, FORD, new int[] {28, 24, 22, 18});

    recordSale(hTable, CHRYSLER, 107);
    recordSale(hTable, FORD, 32);
    recordSale(hTable, FORD, 40);
    recordSale(hTable, TOYOTA, 224);
    recordSale(hTable, CHRYSLER, 113);
    hTable.flushCommits();

    System.out.println(DebugUtil.getContentAsText(hTable));
    verifyLastSalesForAllCompanies(hTable, processor, new int[][]{{113, 107, 105, 110, 115}, {40, 32, 28, 24, 22}, {224, 204, 212, 202}});
    verifyLastSales(hTable, processor, CHRYSLER, new int[] {113, 107, 105, 110, 115});
    verifyLastSales(hTable, processor, FORD, new int[] {40, 32, 28, 24, 22});
    verifyLastSales(hTable, processor, TOYOTA, new int[] {224, 204, 212, 202});
  }

  private static void recordSale(HTable hTable, byte[] company, int price) throws InterruptedException, IOException {
    Put put = new HutPut(company);
    put.add(SALE_CF, Bytes.toBytes("lastPrice0"), Bytes.toBytes(price));
    Thread.sleep(1); // sanity interval
    hTable.put(put);

  }

  private static void verifyLastSales(HTable hTable, UpdateProcessor updateProcessor, byte[] company, int[] prices) throws Throwable {
    ResultScanner resultScanner = new HutCpsResultScanner(hTable, getCompanyScan(company), updateProcessor);
    Result result = resultScanner.next();
    verifyLastSales(result, prices);
  }

  private static Scan getCompanyScan(byte[] company) {
    byte[] stopRow = Arrays.copyOf(company, company.length);
    stopRow[stopRow.length - 1] = (byte) (stopRow[stopRow.length - 1] + 1);
    // setting stopRow to fetch exactly the company needed, otherwise if company's data is absent scan will go further to the next one
    return new Scan(company, stopRow);
  }

  // pricesList - prices for all companies ordered alphabetically
  private static void verifyLastSalesForAllCompanies(HTable hTable, UpdateProcessor updateProcessor, int[][] pricesList) throws Throwable {
    Scan scan = new Scan();
    scan.setCaching(4);
    ResultScanner resultScanner = new HutCpsResultScanner(hTable, scan, updateProcessor);
    Result[] results = resultScanner.next(pricesList.length);
    for (int i = 0; i < pricesList.length; i++) {
      verifyLastSales(results.length > i ? results[i] : null, pricesList[i]);
    }
  }

  private static void verifyLastSales(Result result, int[] prices) {
    // if there's no records yet, then prices are empty
    if (result == null) {
      Assert.assertTrue(prices.length == 0);
      return;
    }

    for (int i = 0; i < 5; i++) {
      byte[] lastStoredPrice = result.getValue(SALE_CF, Bytes.toBytes("lastPrice" + i));
      if (i < prices.length) {
        Assert.assertEquals(prices[i], Bytes.toInt(lastStoredPrice));
      } else {
        Assert.assertNull(lastStoredPrice);
      }
    }
  }
}
