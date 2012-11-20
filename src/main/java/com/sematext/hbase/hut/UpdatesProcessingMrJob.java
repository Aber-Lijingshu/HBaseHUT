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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Perform processing updates by running MapReduce job, and hence utilizes data locality during work.
 * This is a map-only job for doing compaction which means greater utilization of data locality
 * (when reading and writing data), but may cause issues according to some sources (concern is about 
 * writing into same table from a Mapper which may cause issues TODO: really?)
 * NOTE: it may cause some spans of records to be compacted into multiple result
 * records, which is usually (always?) ok.
 */
public final class UpdatesProcessingMrJob {
  private UpdatesProcessingMrJob() {}

  public static class UpdatesProcessingMapper extends TableMapper<ImmutableBytesWritable, Put> {
    public static final String HTABLE_NAME_ATTR = "htable.name";
    public static final String HUT_MR_BUFFER_SIZE_ATTR = "hut.mr.buffer.size";
    public static final String HUT_MR_BUFFER_SIZE_IN_BYTES_ATTR = "hut.mr.buffer.size.bytes";
    public static final String HUT_PROCESSOR_CLASS_ATTR = "hut.processor.class";
    public static final String HUT_PROCESSOR_DETAILS_ATTR = "hut.processor.details";
    public static final String HUT_PROCESSOR_TSMOD_ATTR = "hut.processor.tsMod";
    public static final String HUT_PROCESSOR_MIN_RECORDS_TO_COMPACT_ATTR = "hut.processor.minRecordsToCompact";
    private static final Log LOG = LogFactory.getLog(UpdatesProcessingMapper.class);

    private HTable hTable;

    // buffer for items read by map()
    // can be overridden by HUT_MR_BUFFER_SIZE_ATTR attribute in configuration
    private int bufferMaxSize = 1000;

    // buffer for items read by map()
    // can be overridden by HUT_MR_BUFFER_SIZE_IN_BYTES_ATTR attribute in configuration
    private int bufferMaxSizeInBytes = 32 * 1024 * 1024; // 32 MB

    // TODO: describe
    // can be overridden by HUT_PROCESSOR_TSMOD_ATTR attribute in configuration
    private long tsMod = 0;

    // TODO: describe
    // can be overridden by HUT_PROCESSOR_MIN_RECORDS_TO_COMPACT_ATTR attribute in configuration
    private int minRecordsToCompact = 2;

    // queue with map input records to be fed into updates processor
    private LinkedList<Result> mapInputBuff;
    private long bytesInBuffer;
    // used to process updates TODO: think over reimplementing processing updates for MR case to not stick to scan case
    private DetachedHutResultScanner resultScanner;
    // used to keep last processed update when buffer was emptied (before filled again) - see more comments in the code
    private Put readyToStoreButWaitingFurtherMerging = null;
    private List<byte[]> readyToDelete = new ArrayList<byte[]>();

    // map task state
    private volatile boolean failed;

    // map task counters
    private int writtenRecords = 0;
    private int deletedRecords = 0;

    /**
     * Detached from HTable and ResultScanner scanner that is being fed with {@link org.apache.hadoop.hbase.client.Result} items.
     * Differs from HutResultScanner which uses normal HBase ResultScanner to fetch the data.
     */
    class DetachedHutResultScanner extends HutResultScanner {
      private Put processedUpdatesToStore = null;
      private List<byte[]> rowsToDelete = new ArrayList<byte[]>();

      public DetachedHutResultScanner(UpdateProcessor updateProcessor) {
        super(null, updateProcessor, null, true);
      }

      @Override
      protected boolean isMergeNeeded(byte[] firstKey, byte[] secondKey) {
        if (tsMod <= 0) {
          return super.isMergeNeeded(firstKey, secondKey);
        } else {
          return HutRowKeyUtil.sameOriginalKeys(firstKey, secondKey, tsMod);
        }
      }

      @Override
      void store(Put put) throws IOException {
        processedUpdatesToStore = put;
      }

      @Override
      void deleteProcessedRecords(List<byte[]> rows) throws IOException {
        rowsToDelete = rows;
      }

      @Override
      protected void verifyInitParams(ResultScanner resultScanner, UpdateProcessor updateProcessor,
                                      HTable hTable, boolean storeProcessedUpdates) {
        if (updateProcessor == null) {
          throw new IllegalArgumentException("UpdateProcessor should NOT be null.");
        }
        // since this is "detached" scanner, ResultScanner and/or HTable can be null
      }

      @Override
      public Result next() throws IOException {
        processedUpdatesToStore = null;
        rowsToDelete.clear();

        return super.next();
      }

      protected Result fetchNext() throws IOException {
        return fetchNextFromBuffer();
      }

      public Put getProcessedUpdatesToStore() {
        return processedUpdatesToStore;
      }

      public List<byte[]> getRowsToDelete() {
        return rowsToDelete;
      }
    }

    /**
     * Pass the key, value to reduce.
     *
     * @param key  The current key.
     * @param value  The current value.
     * @param context  The current context.
     * @throws java.io.IOException When writing the record fails.
     * @throws InterruptedException When the job is aborted.
     */
    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
      addToMapInputBuffer(value);
      if (!isMapInputBufferFull()) {
        return;
      }

      // more or less reasonable: attempt to ping every time when switching to process buffered records
      pingMap(context); // TODO: allow user to control pinging

      // processing buffered rows
      try {
        Result res = resultScanner.next();
        Result prev = null;
        Put processingResultToStore = null;
        List<byte[]> toDeleteAfterStoringProcessingResult = new ArrayList<byte[]>();
        while (res != null) {
          // we save previous record in case processingResultToStore is null and this is the last
          // element in buffer. In that case we will put it back to buffer to give it a chance to merge
          // with next items coming to map method
          prev = res;

          // if merging occurred, it will be stored as processingResultToStore
          processingResultToStore = resultScanner.getProcessedUpdatesToStore();
          toDeleteAfterStoringProcessingResult.addAll(resultScanner.getRowsToDelete());

          // last processing result from previous buffered records
          // got chance to be merged, but looks like next record is from different group (i.e. no merge occurred)
          if (processingResultToStore == null && readyToStoreButWaitingFurtherMerging != null) {
            store(readyToStoreButWaitingFurtherMerging, readyToDelete);
            readyToDelete.clear();
          }
          // setting to null in any case:
          // * either it was written above or
          // * was merged with next records and will be written below
          readyToStoreButWaitingFurtherMerging = null;
          // in case readyToStoreButWaitingFurtherMerging was merged with next records, we need to
          // "transfer" those records we wanted to delete with it
          toDeleteAfterStoringProcessingResult.addAll(readyToDelete);
          readyToDelete.clear();

          res = resultScanner.next();
          boolean lastInBuffer = res == null;
          // We don't want to store last processed result *now*,
          // instead we postpone storing it to give it a chance to merge with next map input records down the road.
          if (!lastInBuffer) {
            if (processingResultToStore != null) {
              store(processingResultToStore, toDeleteAfterStoringProcessingResult);
              toDeleteAfterStoringProcessingResult.clear();
            }
          }
        }

        if (prev != null) {
          // see explanation near assignment
          boolean added = addToMapInputBufferIfSpaceAvailable(prev);

          if (!added && processingResultToStore != null) {
            store(processingResultToStore, toDeleteAfterStoringProcessingResult);
            toDeleteAfterStoringProcessingResult.clear();
            return;
          }

          readyToStoreButWaitingFurtherMerging = processingResultToStore;
          readyToDelete.addAll(toDeleteAfterStoringProcessingResult);
        }
      } catch (IOException e) {
        LOG.error(e);
        // TODO: do we really want to fail the whole job? or just skip processing these group of updates
        failed = true; // marking job as failed
      } catch (InterruptedException e) {
        LOG.error(e);
        // TODO: do we really want to fail the whole job? or just skip processing these group of updates
        failed = true; // marking job as failed
      }

    }

    private Result fetchNextFromBuffer() {
      if (mapInputBuff.size() == 0) {
        return null;
      }

      Result r = mapInputBuff.poll();
      bytesInBuffer -= getSize(r);
      return r;
    }

    private void addToMapInputBuffer(Result value) {
      bytesInBuffer += getSize(value);
      mapInputBuff.addLast(value);
    }

    private boolean addToMapInputBufferIfSpaceAvailable(Result value) {
      if (bytesInBuffer + getSize(value) <= bufferMaxSizeInBytes) {
        addToMapInputBuffer(value);
        return true;
      }

      return false;
    }

    private static int getSize(Result value) {
      // TODO: is this the best way of calculating size? Tried using getBytes() but it sometimes returns null
      int size = 0;
      for (KeyValue kv : value.raw()) {
        size += kv.getLength();
      }

      return size;
    }

    private boolean isMapInputBufferFull() {
      return mapInputBuff.size() >= bufferMaxSize || bytesInBuffer >= bufferMaxSizeInBytes;
    }

    private void store(Put processingResultToStore, List<byte[]> rowsToDeleteAfterStoringProcessingResult)
            throws IOException, InterruptedException {
      hTable.put(processingResultToStore);
      writtenRecords++;
      HutResultScanner.deleteProcessedRecords(hTable, rowsToDeleteAfterStoringProcessingResult);
      deletedRecords += rowsToDeleteAfterStoringProcessingResult.size();
    }

    private long lastPingTimeMap = System.currentTimeMillis();

    public void pingMap(Context context) {
      final long currtime = System.currentTimeMillis();
      if (currtime - lastPingTimeMap > 2000) {
        context.progress();
        lastPingTimeMap = currtime;
      }
    }

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
      super.setup(context);

      String updatesProcessorClass =  context.getConfiguration().get(HUT_PROCESSOR_CLASS_ATTR);
      if (updatesProcessorClass == null) {
        throw new IllegalStateException("hut.processor.class missed in the configuration");
      }

      String updatesProcessorDetails =  context.getConfiguration().get(HUT_PROCESSOR_DETAILS_ATTR);
      if (updatesProcessorDetails == null) {
        // TODO: throw exception in future versions
        LOG.warn("hut.processor.details missed in the configuration, updates processor serialized state is missing");
      }

      UpdateProcessor updateProcessor = convertStringToUpdateProcessor(updatesProcessorClass, updatesProcessorDetails);
      LOG.info("Using updateProcessor: " + updateProcessor.toString());

      if (updateProcessor instanceof Configurable) {
        ((Configurable) updateProcessor).configure(context.getConfiguration());
      }

      if (updateProcessor instanceof MapContextAware) {
        ((MapContextAware) updateProcessor).setContext(context);
      }

      String bufferSizeValue =  context.getConfiguration().get(HUT_MR_BUFFER_SIZE_ATTR);
      if (bufferSizeValue == null) {
        LOG.info(HUT_MR_BUFFER_SIZE_ATTR + " is missed in the configuration, using default value: " + bufferMaxSize);
      } else {
        bufferMaxSize = Integer.valueOf(bufferSizeValue);
        LOG.info("Using bufferMaxSize: " + bufferMaxSize);
      }

      String bufferMaxSizeInBytesValue =  context.getConfiguration().get(HUT_MR_BUFFER_SIZE_IN_BYTES_ATTR);
      if (bufferMaxSizeInBytesValue == null) {
        LOG.info(HUT_MR_BUFFER_SIZE_IN_BYTES_ATTR + " is missed in the configuration, using default value: " + bufferMaxSizeInBytes);
      } else {
        bufferMaxSizeInBytes = Integer.valueOf(bufferMaxSizeInBytesValue);
        LOG.info("Using bufferMaxSizeInBytes: " + bufferMaxSizeInBytes);
      }

      String tsModValue =  context.getConfiguration().get(HUT_PROCESSOR_TSMOD_ATTR);
      if (tsModValue == null) {
        LOG.info(HUT_PROCESSOR_TSMOD_ATTR + " is missed in the configuration, using default value: " + tsMod);
      } else {
        tsMod = Long.valueOf(tsModValue);
        LOG.info("Using tsMod: " + tsMod);
      }

      String minRecordsToCompactValue =  context.getConfiguration().get(HUT_PROCESSOR_MIN_RECORDS_TO_COMPACT_ATTR);
      if (minRecordsToCompactValue == null) {
        LOG.info(HUT_PROCESSOR_MIN_RECORDS_TO_COMPACT_ATTR +
                " is missed in the configuration, using default value: " + minRecordsToCompact);
      } else {
        minRecordsToCompact = Integer.valueOf(minRecordsToCompactValue);
        LOG.info("Using minRecordsToCompact: " + minRecordsToCompact);
      }

      // TODO: add validation of configuration attributes
      String tableName = context.getConfiguration().get(HTABLE_NAME_ATTR);
      if (tableName == null) {
        throw new IllegalStateException(HTABLE_NAME_ATTR + " missed in the configuration");
      }
      hTable = new HTable(tableName);
      // NOTE: we are OK with using client-side buffer as losing deletes will not corrupt the data
      // TODO: make these settings configurable
      hTable.setAutoFlush(false);
      // 4MB
      hTable.setWriteBufferSize(4 * 1024 * 1024);


      mapInputBuff = new LinkedList<Result>();
      bytesInBuffer = 0;
      resultScanner = new DetachedHutResultScanner(updateProcessor);
      resultScanner.setMinRecordsToProcess(minRecordsToCompact);
      failed = false;

      // map task counters
      writtenRecords = 0;
      deletedRecords = 0;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      if (mapInputBuff.size() > 0) {
        Result res = resultScanner.next();
        while (res != null) {
          Put processingResultToStore = resultScanner.getProcessedUpdatesToStore();
          List<byte[]> toDeleteAfterStoringProcessingResult = resultScanner.getRowsToDelete();

          // last processing result from previous buffered records
          // got chance to be merged, but looks like next record is from different group (i.e. no merge occurred)
          if (processingResultToStore == null && readyToStoreButWaitingFurtherMerging != null) {
            store(readyToStoreButWaitingFurtherMerging, readyToDelete);
            readyToDelete.clear();
          }

          if (processingResultToStore != null) {
            // in case readyToStoreButWaitingFurtherMerging was merged with next records, we need to
            // "transfer" those records we wanted to delete with it
            toDeleteAfterStoringProcessingResult.addAll(readyToDelete);
            readyToDelete.clear();
            store(processingResultToStore, toDeleteAfterStoringProcessingResult);
            toDeleteAfterStoringProcessingResult.clear();
          }
          res = resultScanner.next();
        }

      }

      mapInputBuff.clear();
      bytesInBuffer = 0;

      context.getCounter("hut_compaction", "writtenRecords").increment(writtenRecords);
      context.getCounter("hut_compaction", "deletedRecords").increment(deletedRecords);


      if (failed) {
        throw new RuntimeException("Job was marked as failed");
      }

      hTable.close();

      super.cleanup(context);
    }
  }

  /**
   * Use this before submitting a TableMap job. It will appropriately set up
   * the job.
   *
   * @param table  The table name.
   * @param scan  The scan with the columns to scan.
   * @param up update processor implementation
   * @param job  The job configuration.
   * @throws java.io.IOException When setting up the job fails.
   */
  @SuppressWarnings("unchecked")
  public static void initJob(String table, Scan scan, UpdateProcessor up, Job job)
          throws IOException {
    TableMapReduceUtil.initTableMapperJob(table, scan, UpdatesProcessingMapper.class, null, null, job);
    job.setJarByClass(UpdatesProcessingMrJob.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);

    job.getConfiguration().set(UpdatesProcessingMapper.HTABLE_NAME_ATTR, table);
    job.getConfiguration().set(UpdatesProcessingMapper.HUT_PROCESSOR_CLASS_ATTR, up.getClass().getName());
    job.getConfiguration().set(UpdatesProcessingMapper.HUT_PROCESSOR_DETAILS_ATTR, convertUpdateProcessorToString(up));

    job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false"); // TODO: explain
  }

  /**
   * Writes the given updatesProcessor into a Base64 encoded string.
   *
   * @param up  The updatesProcessor to write out.
   * @return The updateProcessor saved in a Base64 encoded string.
   * @throws java.io.IOException When writing the updateProcessor fails.
   */
  static String convertUpdateProcessorToString(UpdateProcessor up) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(out);
    up.write(dos);
    return Base64.encodeBytes(out.toByteArray());
  }

  /**
   * Converts the given Base64 string back into a UpdateProcessor instance.
   *
   * @param upClassName  The updateProcessor class name.
   * @param base64  The updateProcessor details.
   * @return The newly created updateProcessor instance.
   * @throws java.io.IOException When reading the updateProcessor instance fails.
   */
  static UpdateProcessor convertStringToUpdateProcessor(String upClassName, String base64)
          throws IOException {
    UpdateProcessor up = createInstance(upClassName, UpdateProcessor.class);
    if (base64 != null) {
      ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(base64));
      DataInputStream dis = new DataInputStream(bis);
      up.readFields(dis);
    }
    return up;
  }

  @SuppressWarnings ({"unchecked", "unused"})
  private static <T>T createInstance(String className, Class<T> clazz) {
    try {
      Class c = Class.forName(className);
      return (T) c.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("Could not create class instance.", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Could not create class instance.", e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not create class instance.", e);
    }
  }
}
