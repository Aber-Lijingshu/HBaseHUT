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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import com.sematext.hbase.hut.UpdateProcessor;

/**
 * HBaseHut {@link org.apache.hadoop.hbase.client.ResultScanner} implementation
 * that makes use of CPs to fetch the data.
 * TODO: This is just proof-of-concept implementation, should be revised from performance to improve performance
 * This is not a thread-safe object (TODO: explain).
 */
// TODO: ensure unit-tests cover multi-region case
public class HutCpsResultScanner implements ResultScanner {
  // Underlying map is a TreeMap (refer to implementation of HTable.coprocessorExec) - TODO: is it safe to rely on it?
  // (values sorted by key:
  private final Map<byte[], List<Result>> recordsFromRegions;

  private boolean exhausted;

  public HutCpsResultScanner(HTable hTable, Scan scan, final UpdateProcessor processor) throws Throwable {
    recordsFromRegions = get(hTable, scan, processor);
    exhausted = recordsFromRegions.size() <= 0;
  }

  @Override
  public Result next() throws IOException {
    if (exhausted) {
      return null;
    }

    // TODO: is this check really needed?
    if (recordsFromRegions.size() == 0) {
      exhausted = true;
      return null;
    }

    Collection<List<Result>> values = recordsFromRegions.values();
    List<List<Result>> toDelete = null;
    Result next = null;
    for (List<Result> list : values) {
      if (list.size() == 0) {
        if (toDelete == null) {
          toDelete = new ArrayList<List<Result>>();
        }

        toDelete.add(list);
        continue;
      }

      next = list.get(0);
      list.remove(next);
    }

    if (next == null) {
      exhausted = true;
      return null;
    }

    return next;
  }

  @Override
  public Result[] next(int nbRows) throws IOException {
    if (exhausted) {
      return new Result[0];
    }
    List<Result> list = new ArrayList<Result>();
    for (int i = 0; i < nbRows; i++) {
      Result res = next();

      if (res == null) {
        break;
      }

      list.add(res);
    }

    return list.toArray(new Result[list.size()]);
  }

  @Override
  public void close() {
    // DO NOTHING
  }

  // Implementation is identical to HTable.ClientScanner.iterator()
  @Override
  public Iterator<Result> iterator() {
    return new Iterator<Result>() {
      // The next RowResult, possibly pre-read
      Result next = null;

      // return true if there is another item pending, false if there isn't.
      // this method is where the actual advancing takes place, but you need
      // to call next() to consume it. hasNext() will only advance if there
      // isn't a pending next().
      public boolean hasNext() {
        if (next == null) {
          try {
            next = HutCpsResultScanner.this.next();
            return next != null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      }

      // get the pending next item and advance the iterator. returns null if
      // there is no next item.
      public Result next() {
        // since hasNext() does the real advancing, we call this to determine
        // if there is a next before proceeding.
        if (!hasNext()) {
          return null;
        }

        // if we get to here, then hasNext() has given us an item to return.
        // we want to return the item and then null out the next pointer, so
        // we use a temporary variable.
        Result temp = next;
        next = null;
        return temp;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private Map<byte[], List<Result>> get(HTable hTable, final Scan scan, final UpdateProcessor processor) throws Throwable {
    return hTable.coprocessorService(HutReadProtocol.class, scan.getStartRow(), scan.getStopRow(),
        new Batch.Call<HutReadProtocol, List<Result>>() {
          public List<Result> call(HutReadProtocol instance) throws IOException {
            return instance.get(scan, processor);
          }
        });
  }
}
