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

package org.apache.hadoop.mapreduce.split;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.split.JobSplit.SplitMetaInfo;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The class that is used by the Job clients to write splits (both the meta
 * and the raw bytes parts)
 * 对job中split进行拆分存储
 * 该类在JobSubmitter进行,即job提交的时候就进行拆分好了
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobSplitWriter {

  private static final Log LOG = LogFactory.getLog(JobSplitWriter.class);
  private static final int splitVersion = JobSplit.META_SPLIT_VERSION;//版本号
  private static final byte[] SPLIT_FILE_HEADER;//魔字符

  static {
    try {
      SPLIT_FILE_HEADER = "SPL".getBytes("UTF-8");
    } catch (UnsupportedEncodingException u) {
      throw new RuntimeException(u);
    }
  }
  
  /**
   * 1.写入job.split文件
   * 2.向job.splitmetainfo文件写入信息
   */
  @SuppressWarnings("unchecked")
  public static <T extends InputSplit> void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, List<InputSplit> splits) 
  throws IOException, InterruptedException {
    T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);
    createSplitFiles(jobSubmitDir, conf, fs, array);
  }
  
  /**
   * 1.写入job.split文件
   * 2.向job.splitmetainfo文件写入信息
   */
  public static <T extends InputSplit> void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, T[] splits) 
  throws IOException, InterruptedException {
	  //写入job.split文件,该步骤是创建job.split文件
    FSDataOutputStream out = createFile(fs, 
        JobSubmissionFiles.getJobSplitFile(jobSubmitDir), conf);
    //将每一个InputSplit写入该job.split文件中
    SplitMetaInfo[] info = writeNewSplits(conf, splits, out);
    out.close();
    
    /**
     *  向job.splitmetainfo文件写入信息,主要是有多少个SplitMetaInfo[]
     */
    writeJobSplitMetaInfo(fs,JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir), 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION), splitVersion,
        info);
  }
  
  /**
   * 1.写入job.split文件
   * 2.向job.splitmetainfo文件写入信息
   * 
   * 注意,该版本是兼容老版本使用
   */
  public static void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, 
      org.apache.hadoop.mapred.InputSplit[] splits) 
  throws IOException {
    FSDataOutputStream out = createFile(fs, 
        JobSubmissionFiles.getJobSplitFile(jobSubmitDir), conf);
    SplitMetaInfo[] info = writeOldSplits(splits, out, conf);
    out.close();
    writeJobSplitMetaInfo(fs,JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir), 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION), splitVersion,
        info);
  }
  
  private static FSDataOutputStream createFile(FileSystem fs, Path splitFile, 
      Configuration job)  throws IOException {
    FSDataOutputStream out = FileSystem.create(fs, splitFile, 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
    int replication = job.getInt(Job.SUBMIT_REPLICATION, 10);
    fs.setReplication(splitFile, (short)replication);
    writeSplitHeader(out);
    return out;
  }
  private static void writeSplitHeader(FSDataOutputStream out) 
  throws IOException {
    out.write(SPLIT_FILE_HEADER);
    out.writeInt(splitVersion);
  }
  
  /**
   * 向out(job.split文件)输入流中写入InputSplit数组信息
   * 写入内容
   * 1.String:split.getClass().getName()
   * 2.该split序列化内容写入out中
   * 3.对这些split对象用SplitMetaInfo存储
   */
  @SuppressWarnings("unchecked")
  private static <T extends InputSplit> 
  SplitMetaInfo[] writeNewSplits(Configuration conf, 
      T[] array, FSDataOutputStream out)
  throws IOException, InterruptedException {

    SplitMetaInfo[] info = new SplitMetaInfo[array.length];
    if (array.length != 0) {
      SerializationFactory factory = new SerializationFactory(conf);
      int i = 0;
      //最多一个split有多少个location
      int maxBlockLocations = conf.getInt(MRConfig.MAX_BLOCK_LOCATIONS_KEY,
          MRConfig.MAX_BLOCK_LOCATIONS_DEFAULT);
      long offset = out.getPos();
      for(T split: array) {
        long prevCount = out.getPos();
        Text.writeString(out, split.getClass().getName());
        Serializer<T> serializer = 
          factory.getSerializer((Class<T>) split.getClass());
        serializer.open(out);
        serializer.serialize(split);
        long currCount = out.getPos();
        String[] locations = split.getLocations();
        if (locations.length > maxBlockLocations) {
          LOG.warn("Max block location exceeded for split: "
              + split + " splitsize: " + locations.length +
              " maxsize: " + maxBlockLocations);
          locations = Arrays.copyOf(locations, maxBlockLocations);
        }
        info[i++] = 
          new JobSplit.SplitMetaInfo( 
              locations, offset,
              split.getLength());
        offset += currCount - prevCount;
      }
    }
    return info;
  }
  
  private static SplitMetaInfo[] writeOldSplits(
      org.apache.hadoop.mapred.InputSplit[] splits,
      FSDataOutputStream out, Configuration conf) throws IOException {
    SplitMetaInfo[] info = new SplitMetaInfo[splits.length];
    if (splits.length != 0) {
      int i = 0;
      long offset = out.getPos();
      int maxBlockLocations = conf.getInt(MRConfig.MAX_BLOCK_LOCATIONS_KEY,
          MRConfig.MAX_BLOCK_LOCATIONS_DEFAULT);
      for(org.apache.hadoop.mapred.InputSplit split: splits) {
        long prevLen = out.getPos();
        Text.writeString(out, split.getClass().getName());
        split.write(out);
        long currLen = out.getPos();
        String[] locations = split.getLocations();
        if (locations.length > maxBlockLocations) {
          LOG.warn("Max block location exceeded for split: "
              + split + " splitsize: " + locations.length +
              " maxsize: " + maxBlockLocations);
          locations = Arrays.copyOf(locations,maxBlockLocations);
        }
        info[i++] = new JobSplit.SplitMetaInfo( 
            locations, offset,
            split.getLength());
        offset += currLen - prevLen;
      }
    }
    return info;
  }

  /**
   * 将该map的split信息的偏移量存储到文件中
   */
  private static void writeJobSplitMetaInfo(FileSystem fs, Path filename, 
      FsPermission p, int splitMetaInfoVersion, 
      JobSplit.SplitMetaInfo[] allSplitMetaInfo) 
  throws IOException {
    // write the splits meta-info to a file for the job tracker
    FSDataOutputStream out = 
      FileSystem.create(fs, filename, p);
    out.write(JobSplit.META_SPLIT_FILE_HEADER);
    WritableUtils.writeVInt(out, splitMetaInfoVersion);
    WritableUtils.writeVInt(out, allSplitMetaInfo.length);
    for (JobSplit.SplitMetaInfo splitMetaInfo : allSplitMetaInfo) {
      splitMetaInfo.write(out);
    }
    out.close();
  }
}

