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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A Convenience class that creates output lazily.
 * Use in conjuction with org.apache.hadoop.mapreduce.lib.output.MultipleOutputs to recreate the
 * behaviour of org.apache.hadoop.mapred.lib.MultipleTextOutputFormat (etc) of the old Hadoop API.
 * See {@link MultipleOutputs} documentation for more information.
 * 当文件尚未被写入key-value的时候,是不会被创建对应的输出文件的,防止空文件过多,占用namenode内存
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LazyOutputFormat <K,V> extends FilterOutputFormat<K, V> {
  public static String OUTPUT_FORMAT = 
    "mapreduce.output.lazyoutputformat.outputformat";//真正意义上的输出格式化对象
  /**
   * Set the underlying output format for LazyOutputFormat.
   * @param job the {@link Job} to modify
   * @param theClass the underlying class
   * 设置该输出为LazyOutputFormat,并且设置真正的输出类是theClass,该类是由LazyOutputFormat系统调用的
   */
  @SuppressWarnings("unchecked")
  public static void  setOutputFormatClass(Job job, 
                                     Class<? extends OutputFormat> theClass) {
      job.setOutputFormatClass(LazyOutputFormat.class);
      job.getConfiguration().setClass(OUTPUT_FORMAT, 
          theClass, OutputFormat.class);
  }

  //获取真正的格式化对象作为基类
  @SuppressWarnings("unchecked")
  private void getBaseOutputFormat(Configuration conf) 
  throws IOException {
    baseOut =  ((OutputFormat<K, V>) ReflectionUtils.newInstance(
      conf.getClass(OUTPUT_FORMAT, null), conf));
    if (baseOut == null) {
      throw new IOException("Output Format not set for LazyOutputFormat");
    }
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
  throws IOException, InterruptedException {
    if (baseOut == null) {
      getBaseOutputFormat(context.getConfiguration());
    }
    return new LazyRecordWriter<K, V>(baseOut, context);
  }
  
  @Override
  public void checkOutputSpecs(JobContext context) 
  throws IOException, InterruptedException {
    if (baseOut == null) {
      getBaseOutputFormat(context.getConfiguration());
    }
   super.checkOutputSpecs(context);
  }
  
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) 
  throws IOException, InterruptedException {
    if (baseOut == null) {
      getBaseOutputFormat(context.getConfiguration());
    }
    return super.getOutputCommitter(context);
  }
  
  /**
   * A convenience class to be used with LazyOutputFormat
   * 在真正格式化的类前面做了一个过滤器
   */
  private static class LazyRecordWriter<K,V> extends FilterRecordWriter<K,V> {

    final OutputFormat<K,V> outputFormat;//在真正格式化的类前面做了一个过滤器做为输出
    final TaskAttemptContext taskContext;//执行该任务的task上下文环境

    public LazyRecordWriter(OutputFormat<K,V> out, 
                            TaskAttemptContext taskContext)
    throws IOException, InterruptedException {
      this.outputFormat = out;
      this.taskContext = taskContext;
    }

    /***
     * 当文件尚未被写入key-value的时候,是不会被创建对应的输出文件的,防止空文件过多,占用namenode内存
     */
    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      if (rawWriter == null) {
        rawWriter = outputFormat.getRecordWriter(taskContext);
      }
      rawWriter.write(key, value);
    }

    @Override
    public void close(TaskAttemptContext context) 
    throws IOException, InterruptedException {
      if (rawWriter != null) {
        rawWriter.close(context);
      }
    }

  }
}
