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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.sematext.hbase.hut.ByteArrayWrapper;
import com.sematext.hbase.hut.UpdateProcessingResult;
import com.sematext.hbase.hut.UpdateProcessor;

/**
 * Simple updates processor which calculates sum for values in every column.
 * Values in columns should be encoded as Bytes.toLong(long).
 * TODO: since deltas are always small, it makes much more sense to use values encoded as Bytes.bytesToVint()
 *
 * NOTE: this is example implementation, may be not optimal one, treat it as example
 */
public class SumColumnsUp extends UpdateProcessor {
  private Collection<Pair<ByteArrayWrapper, ByteArrayWrapper>> columns = null;

  /**
   * Writable constructor, do NOT use it directly
   */
  public SumColumnsUp() {
  }

  public SumColumnsUp(Pair<byte[], byte[]>... columns) {
    Collection<Pair<ByteArrayWrapper, ByteArrayWrapper>> cols =
            new ArrayList<Pair<ByteArrayWrapper, ByteArrayWrapper>>(columns.length);
    for (Pair<byte[], byte[]> column : columns) {
      cols.add(new Pair<ByteArrayWrapper, ByteArrayWrapper>(
              new ByteArrayWrapper(column.getFirst()), new ByteArrayWrapper(column.getSecond())));
    }
    this.columns = cols;
  }

  public void process(Iterable<Result> records, UpdateProcessingResult processingResult) {
    // NOTE: this is example implementation, not most optimal one.
    //       It looks like at least changing to iterating over colfams first would be faster

    // TODO: allow using processingResult as a map for that purpose?
    Map<ByteArrayWrapper, Map<ByteArrayWrapper, Long>> resultMap =
            new HashMap<ByteArrayWrapper, Map<ByteArrayWrapper, Long>>();
    for (Result row : records) {
      for (Pair<ByteArrayWrapper, ByteArrayWrapper> column : columns) {
        ByteArrayWrapper colfamKey = column.getFirst();
        byte[] colfam = colfamKey.getBytes();
        ByteArrayWrapper qualKey = column.getSecond();
        byte[] qual = qualKey.getBytes();
        byte[] value = row.getValue(colfam, qual);
        if (value == null || value.length != Bytes.SIZEOF_LONG) {
          continue;
        }

        long val = Bytes.toLong(value);

        Map<ByteArrayWrapper, Long> colMap = resultMap.get(colfamKey);

        if (colMap == null) {
          colMap = new HashMap<ByteArrayWrapper, Long>();
          resultMap.put(colfamKey, colMap);
          colMap.put(qualKey, val);
          continue;
        }

        Long v = colMap.get(qualKey);
        v = v == null ? val : v + val;
        // TODO: consider using mutable Long instead (less objects created, less map change operation)
        colMap.put(qualKey, v);
      }
    }

    for (Map.Entry<ByteArrayWrapper, Map<ByteArrayWrapper, Long>> colfamEntry : resultMap.entrySet()) {
      for (Map.Entry<ByteArrayWrapper, Long> qualEntry : colfamEntry.getValue().entrySet()) {
        processingResult.add(colfamEntry.getKey().getBytes(), qualEntry.getKey().getBytes(),
                             Bytes.toBytes(qualEntry.getValue()));
      }
    }
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    if (columns == null) {
      dataOutput.writeInt(0);
      return;
    }

    dataOutput.writeInt(columns.size());
    for (Pair<ByteArrayWrapper, ByteArrayWrapper> column : columns) {
      // colfam
      Bytes.writeByteArray(dataOutput, column.getFirst().getBytes());
      // qualifier
      Bytes.writeByteArray(dataOutput, column.getSecond().getBytes());
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int columnsCount = dataInput.readInt();
    columns = new ArrayList<Pair<ByteArrayWrapper, ByteArrayWrapper>>(columnsCount);
    for (int i = 0; i < columnsCount; i++) {
      byte[] colfam = Bytes.readByteArray(dataInput);
      byte[] qual = Bytes.readByteArray(dataInput);
      Pair<ByteArrayWrapper, ByteArrayWrapper> column =
              new Pair<ByteArrayWrapper, ByteArrayWrapper>(new ByteArrayWrapper(colfam), new ByteArrayWrapper(qual));
      columns.add(column);
    }
  }
}
