/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This class is similar to SequenceFileInputFormat, except it generates
 * SequenceFileAsTextRecordReader which converts the input keys and values
 * to their String forms by calling toString() method. 
 * 读取序列化文件,只是返回值不再是泛型,而是具体的类型,key和value都是Text类型的时候使用该类去读取
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileAsTextInputFormat
  extends SequenceFileInputFormat<Text, Text> {

  public SequenceFileAsTextInputFormat() {
    super();
  }

  public RecordReader<Text, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    context.setStatus(split.toString());
    return new SequenceFileAsTextRecordReader();
  }
}
