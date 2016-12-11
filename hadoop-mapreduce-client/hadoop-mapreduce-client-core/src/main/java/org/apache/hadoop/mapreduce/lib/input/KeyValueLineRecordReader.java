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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This class treats a line in the input as a key/value pair separated by a 
 * separator character. The separator can be specified in config file 
 * under the attribute name mapreduce.input.keyvaluelinerecordreader.key.value.separator. The default
 * separator is the tab character ('\t').
 * 
 * key不再是偏移量了,而是具体的Text内容
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class KeyValueLineRecordReader extends RecordReader<Text, Text> {
  public static final String KEY_VALUE_SEPERATOR = 
    "mapreduce.input.keyvaluelinerecordreader.key.value.separator";//key-value的拆分符号,只能是一个字节
  
  private final LineRecordReader lineRecordReader;

  private byte separator = (byte) '\t';//默认是tab拆分

  private Text innerValue;

  private Text key;
  
  private Text value;
  
  public Class getKeyClass() { return Text.class; }
  
  public KeyValueLineRecordReader(Configuration conf)
    throws IOException {
    
    lineRecordReader = new LineRecordReader();
    String sepStr = conf.get(KEY_VALUE_SEPERATOR, "\t");
    this.separator = (byte) sepStr.charAt(0);
  }

  public void initialize(InputSplit genericSplit,
      TaskAttemptContext context) throws IOException {
    lineRecordReader.initialize(genericSplit, context);//对数据块进行初始化
  }
  
  //找到第一次出现分隔符的位置
  public static int findSeparator(byte[] utf, int start, int length, 
      byte sep) {
    for (int i = start; i < (start + length); i++) {
      if (utf[i] == sep) {
        return i;
      }
    }
    return -1;
  }

  //设置key-value的内容,从line一行内容中,第pos位置开始截断,一部分是key,一部分是value
  public static void setKeyValue(Text key, Text value, byte[] line,
      int lineLen, int pos) {
    if (pos == -1) {//说明没有分隔符,则都是key,value为空
      key.set(line, 0, lineLen);
      value.set("");
    } else {//设置key和value
      key.set(line, 0, pos);
      value.set(line, pos + 1, lineLen - pos - 1);
    }
  }
  /** Read key/value pair in a line. */
  public synchronized boolean nextKeyValue()
    throws IOException {
    byte[] line = null;//一行内容
    int lineLen = -1;//一行一共多少个字节
    if (lineRecordReader.nextKeyValue()) {//读取一行内容
      innerValue = lineRecordReader.getCurrentValue();//获取一行的信息
      line = innerValue.getBytes();//一行内容
      lineLen = innerValue.getLength();//一行一共多少个字节
    } else {
      return false;
    }
    if (line == null)
      return false;
    if (key == null) {
      key = new Text();
    }
    if (value == null) {
      value = new Text();
    }
    int pos = findSeparator(line, 0, lineLen, this.separator);//从一行内容中找到分隔符
    setKeyValue(key, value, line, lineLen, pos);//设置key-value的内容
    return true;
  }
  
  public Text getCurrentKey() {
    return key;
  }

  public Text getCurrentValue() {
    return value;
  }

  public float getProgress() throws IOException {
    return lineRecordReader.getProgress();
  }
  
  public synchronized void close() throws IOException { 
    lineRecordReader.close();
  }
}
