package com.sematext.hbase.agg;

import com.sematext.hbase.hut.UpdateProcessingResult;
import com.sematext.hbase.hut.UpdateProcessor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

// NOTE: This is just stub implementation. Relies on data to be of type double.
public class MaxFunction extends UpdateProcessor {
  private byte[] colfam;
  private byte[] qual;

  public MaxFunction(byte[] colfam, byte[] qual) {
    this.colfam = colfam;
    this.qual = qual;
  }

  @Override
  public void process(Iterable<Result> records, UpdateProcessingResult processingResult) {
    Double maxVal = null;
    // Processing records
    for (Result record : records) {
      byte[] valBytes = record.getValue(colfam, qual);
      if (valBytes != null) {
        double val = Bytes.toDouble(valBytes);
        if (maxVal == null || maxVal < val) {
          maxVal = val;
        }
      }
    }

    // Writing result
    if (maxVal == null) { // nothing to output
      return;
    }

    processingResult.add(colfam, qual, Bytes.toBytes(maxVal));
  }
}

