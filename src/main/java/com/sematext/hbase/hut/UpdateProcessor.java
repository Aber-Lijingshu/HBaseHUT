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

import org.apache.hadoop.hbase.client.Result;

/**
 * Performs records processing.
 * Implementation SHOULD be stateless and thread-safe
 */
public abstract class UpdateProcessor {
  public abstract void process(Iterable<Result> records, UpdateProcessingResult processingResult);

  /**
   * Allows to skip merging of records even if HBase core decided to merge them.
   * Adds extra flexibility to skip unnecessary merging of records.
   *
   * @param originalKey original key of the records about to be merged
   * @return true if merge needed, false otherwise
   */
  public boolean isMergeNeeded(byte[] originalKey) {
    return true;
  }
}
