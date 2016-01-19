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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/** An {@link OutputFormat} that writes {@link SequenceFile}s. 
 * 创建SequenceFile.Writer写入key-value
 */ 
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileOutputFormat <K,V> extends FileOutputFormat<K, V> {

  /**
   * 创建SequenceFile.Writer对象
   * 因为SequenceFile.Writer对象需要key-value方式序列化,因此生成该文件需要key-value对应的class对象
   * @param context 表示运行该job的task环境上下文
   */
  protected SequenceFile.Writer getSequenceWriter(TaskAttemptContext context,
      Class<?> keyClass, Class<?> valueClass) 
      throws IOException {
    Configuration conf = context.getConfiguration();
	    
    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    if (getCompressOutput(context)) {//是否支持压缩
      // find the kind of compression to do 压缩类型,是行压缩,还是段落BLOCK压缩等
      compressionType = getOutputCompressionType(context);
      // find the right codec 压缩方式是gz压缩还是……
      Class<?> codecClass = getOutputCompressorClass(context, 
                                                     DefaultCodec.class);
      codec = (CompressionCodec) 
        ReflectionUtils.newInstance(codecClass, conf);
    }
    // get the path of the temporary output file 
    Path file = getDefaultWorkFile(context, "");
    FileSystem fs = file.getFileSystem(conf);
    return SequenceFile.createWriter(fs, conf, file,
             keyClass,
             valueClass,
             compressionType,
             codec,
             context);
  }
  
  /**
   * 创建SequenceFile.Writer写入key-value
   */
  public RecordWriter<K, V> 
         getRecordWriter(TaskAttemptContext context
                         ) throws IOException, InterruptedException {
    final SequenceFile.Writer out = getSequenceWriter(context,
      context.getOutputKeyClass(), context.getOutputValueClass());

    return new RecordWriter<K, V>() {

        public void write(K key, V value)
          throws IOException {

          out.append(key, value);
        }

        public void close(TaskAttemptContext context) throws IOException { 
          out.close();
        }
      };
  }

  /**
   * Get the {@link CompressionType} for the output {@link SequenceFile}.
   * @param job the {@link Job}
   * @return the {@link CompressionType} for the output {@link SequenceFile}, 
   *         defaulting to {@link CompressionType#RECORD}
   * 压缩类型,是行压缩,还是段落BLOCK压缩等        
   */
  public static CompressionType getOutputCompressionType(JobContext job) {
    String val = job.getConfiguration().get(FileOutputFormat.COMPRESS_TYPE, 
                                            CompressionType.RECORD.toString());
    return CompressionType.valueOf(val);
  }
  
  /**
   * Set the {@link CompressionType} for the output {@link SequenceFile}.
   * @param job the {@link Job} to modify
   * @param style the {@link CompressionType} for the output
   *              {@link SequenceFile} 
   * 设置压缩类型,是行压缩,还是段落BLOCK压缩等
   */
  public static void setOutputCompressionType(Job job, 
		                                          CompressionType style) {
    setCompressOutput(job, true);
    job.getConfiguration().set(FileOutputFormat.COMPRESS_TYPE, 
                               style.toString());
  }

}

