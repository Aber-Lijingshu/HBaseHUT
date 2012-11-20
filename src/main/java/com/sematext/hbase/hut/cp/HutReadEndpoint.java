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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.sematext.hbase.hut.HutResultScanner;
import com.sematext.hbase.hut.UpdateProcessor;

/**
 * Implements {@link HutReadProtocol}
 */
public class HutReadEndpoint extends BaseEndpointCoprocessor implements HutReadProtocol {

    public static class HutResultInternalScanner extends HutResultScanner {
      private InternalScanner internalScanner;
      // NOTE: this can be converted in local variable, but we keep single object instance
      // to avoid creating redundant objects in freq invoked method
      private List<KeyValue> curVals = new ArrayList<KeyValue>();
      private boolean exhausted;

      public HutResultInternalScanner(InternalScanner internalScanner, UpdateProcessor updateProcessor) {
        super(null, updateProcessor);
        this.internalScanner = internalScanner;
        this.exhausted = false;
      }

      @Override
      protected void verifyInitParams(ResultScanner resultScanner, UpdateProcessor updateProcessor, HTable hTable, boolean storeProcessedUpdates) {
        if (updateProcessor == null) {
          throw new IllegalArgumentException("UpdateProcessor should NOT be null.");
        }
        // since this is "detached" scanner, ResultScanner and/or HTable can be null
      }

      @Override
      protected Result fetchNext() throws IOException {
        if (exhausted) {
          return null;
        }

        curVals.clear();
        exhausted = !internalScanner.next(curVals);
        // TODO: place for potential improvement: may be redundant object instance creation
        return new Result(curVals);
      }
    }

    @Override
    public List<Result> get(Scan scan, UpdateProcessor up) throws IOException {
      // aggregate at each region
      List<Result> list = new ArrayList<Result>();
      InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion().getScanner(scan);
      HutResultInternalScanner hutScanner = new HutResultInternalScanner(scanner, up);
      try {
        for (Result res : hutScanner) {
          list.add(res);
        }

      } finally {
        scanner.close();
      }
      return list;
    }
}
