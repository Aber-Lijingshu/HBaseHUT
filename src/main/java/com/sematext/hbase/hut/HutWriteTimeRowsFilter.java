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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;

/**
 * Filters data based on the HBaseHUT record write time.
 * TODO: add unit-tests
 */
public class HutWriteTimeRowsFilter extends FilterBase {
  private long minTs = Long.MIN_VALUE;
  private long maxTs = Long.MAX_VALUE;

  byte[] nextRowKeyHint = null;

  /**
   * Used internally for reflection, do NOT use it directly
   */
  public HutWriteTimeRowsFilter() {
  }

  public HutWriteTimeRowsFilter(long maxTs, long minTs) {
    this.maxTs = maxTs;
    this.minTs = minTs;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {
    byte[] rowKey = kv.getRow();

    if (!HutRowKeyUtil.writtenAfter(rowKey, minTs)) {
      // omits hut data at the end of key and makes copy of the array
      byte[] original = HutRowKeyUtil.getOriginalKey(rowKey);
      // hint is the same original key but start interval is set to minTs
      byte[] hint = HutRowKeyUtil.createNewKey(original, minTs);
      nextRowKeyHint = hint;
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }

    if (!HutRowKeyUtil.writtenBefore(rowKey, maxTs)) {
      // omits hut data at the end of key and makes copy of the array
      byte[] original = HutRowKeyUtil.getOriginalKey(rowKey);
      // hint is the same original key but start interval is set to max long value to fast-forward to next record
      // with different original key
      byte[] hint = HutRowKeyUtil.createNewKey(original, Long.MAX_VALUE);
      nextRowKeyHint = hint;
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }

    return ReturnCode.INCLUDE;
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue currentKV) {
    KeyValue hint =  KeyValue.createFirstOnRow(nextRowKeyHint);
    nextRowKeyHint = null;

    return hint;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(this.minTs);
    dataOutput.writeLong(this.maxTs);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.minTs = dataInput.readLong();
    this.maxTs = dataInput.readLong();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("HutWriteTimeRowsFilter");
    sb.append("{minTs=").append(minTs);
    sb.append(", maxTs=").append(maxTs);
    sb.append('}');
    return sb.toString();
  }
}
