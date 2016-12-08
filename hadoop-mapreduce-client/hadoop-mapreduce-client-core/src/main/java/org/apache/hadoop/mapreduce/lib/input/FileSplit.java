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
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/** A section of an input file.  Returned by {@link
 * InputFormat#getSplits(JobContext)} and passed to
 * {@link InputFormat#createRecordReader(InputSplit,TaskAttemptContext)}.
 * 代表一个map要读取的内容
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileSplit extends InputSplit implements Writable {
  private Path file;
  private long start;
  private long length;//可能该数据块所在节点不能容纳length字节数据,即需要跨数据块读取了,是有这种可能的,而host只是只带一个数据块节点
  private String[] hosts;
  private SplitLocationInfo[] hostInfos;//就是host,一个数据块所在的备份节点,只是其中包含了一个inMemory对象,表示true则说明该节点上的信息存储在内存中了,会更加快被访问

  public FileSplit() {}

  /** Constructs a split with host information
   *
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   * @param hosts the list of hosts containing the block, possibly null
   */
  public FileSplit(Path file, long start, long length, String[] hosts) {
    this.file = file;
    this.start = start;
    this.length = length;
    this.hosts = hosts;
  }
  
  /** Constructs a split with host and cached-blocks information
  *
  * @param file the file name 读取哪个file路径的文件
  * @param start the position of the first byte in the file to process 从第几个字节开始读取
  * @param length the number of bytes in the file to process 读取多少字节
  * @param hosts the list of hosts containing the block 该数据块在哪个节点上
  * @param inMemoryHosts the list of hosts containing the block in memory 内存中包含该数据块的信息地址
   * 代表一个map要读取的内容
  */
 public FileSplit(Path file, long start, long length, String[] hosts,
     String[] inMemoryHosts) {
   this(file, start, length, hosts);
   hostInfos = new SplitLocationInfo[hosts.length];
   for (int i = 0; i < hosts.length; i++) {
     // because N will be tiny, scanning is probably faster than a HashSet
     boolean inMemory = false;//host节点上,内存也有该信息
     for (String inMemoryHost : inMemoryHosts) {
       if (inMemoryHost.equals(hosts[i])) {
         inMemory = true;
         break;
       }
     }
     hostInfos[i] = new SplitLocationInfo(hosts[i], inMemory);
   }
 }
 
  /** The file containing this split's data. */
  public Path getPath() { return file; }
  
  /** The position of the first byte in the file to process. */
  public long getStart() { return start; }
  
  /** The number of bytes in the file to process. */
  @Override
  public long getLength() { return length; }

  @Override
  public String toString() { return file + ":" + start + "+" + length; }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, file.toString());
    out.writeLong(start);
    out.writeLong(length);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    file = new Path(Text.readString(in));
    start = in.readLong();
    length = in.readLong();
    hosts = null;
  }

  @Override
  public String[] getLocations() throws IOException {
    if (this.hosts == null) {
      return new String[]{};
    } else {
      return this.hosts;
    }
  }
  
  @Override
  @Evolving
  public SplitLocationInfo[] getLocationInfo() throws IOException {
    return hostInfos;
  }
}
