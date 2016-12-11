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
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * A reader to read fixed length records from a split.  Record offset is
 * returned as key and the record as bytes is returned in value.
 * 
 * FileInputFormat是一行一行读取数据,每一个map方法按照行进行拆分,而本类是根据固定字节长度为一段内容进行拆分
 * 
 * key是字节偏移量 value是需要的字节数组
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FixedLengthRecordReader
    extends RecordReader<LongWritable, BytesWritable> {
  private static final Log LOG 
      = LogFactory.getLog(FixedLengthRecordReader.class);

  private int recordLength;
  private long start;
  private long pos;
  private long end;
  private long  numRecordsRemainingInSplit;
  private FSDataInputStream fileIn;
  private Seekable filePosition;
  
  
  private LongWritable key;
  private BytesWritable value;
  
  
  private boolean isCompressedInput;
  private Decompressor decompressor;
  
  private InputStream inputStream;

  public FixedLengthRecordReader(int recordLength) {
    this.recordLength = recordLength;
  }

  @Override
  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    final Path file = split.getPath();
    initialize(job, split.getStart(), split.getLength(), file);
  }

  // This is also called from the old FixedLengthRecordReader API implementation
  public void initialize(Configuration job, long splitStart, long splitLength,
                         Path file) throws IOException {
    start = splitStart;
    end = start + splitLength;
    long partialRecordLength = start % recordLength;//余数,比如start=100,每个记录recordLength=6个字节  因此100/6余数是4  也就是说这个split有2个字节已经被别的split使用了,这2个字节是因为一共需要6个字节,前一个split有4个字节,因此使用了这个split2个字节,我们要跳过2个字节
    long numBytesToSkip = 0;//跳过字节数
    if (partialRecordLength != 0) {
      numBytesToSkip = recordLength - partialRecordLength;
    }

    // open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(file);

    CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
    if (null != codec) {
      isCompressedInput = true;	
      decompressor = CodecPool.getDecompressor(codec);
      CompressionInputStream cIn
          = codec.createInputStream(fileIn, decompressor);
      filePosition = cIn;
      inputStream = cIn;
      numRecordsRemainingInSplit = Long.MAX_VALUE;
      LOG.info(
          "Compressed input; cannot compute number of records in the split");
    } else {
      fileIn.seek(start);
      filePosition = fileIn;
      inputStream = fileIn;
      long splitSize = end - start - numBytesToSkip;//计算split真正大小
      numRecordsRemainingInSplit = (splitSize + recordLength - 1)/recordLength;//记录该split有多少个固定大小的数据
      if (numRecordsRemainingInSplit < 0) {
        numRecordsRemainingInSplit = 0;
      }
      LOG.info("Expecting " + numRecordsRemainingInSplit
          + " records each with a length of " + recordLength
          + " bytes in the split with an effective size of "
          + splitSize + " bytes");
    }
    if (numBytesToSkip != 0) {
      start += inputStream.skip(numBytesToSkip);
    }
    this.pos = start;
  }

  @Override
  public synchronized boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    if (value == null) {
      value = new BytesWritable(new byte[recordLength]);
    }
    boolean dataRead = false;
    value.setSize(recordLength);
    byte[] record = value.getBytes();//最终存储一行数据的字节数量
    
    if (numRecordsRemainingInSplit > 0) {//说明还有记录可以被读取
      key.set(pos);
      
      int offset = 0;//已经读取了多少个字节在value的缓冲池里面
      int numBytesToRead = recordLength;//要求读取多少个字节
      
      int numBytesRead = 0;//真正读取了多少个字节
      
      while (numBytesToRead > 0) {
        numBytesRead = inputStream.read(record, offset, numBytesToRead);//从inputStream中读取字节,存储到record中,从0位置开始存储,存储numBytesToRead个字节为止
        if (numBytesRead == -1) {
          // EOF
          break;
        }
        offset += numBytesRead;//移动value缓冲池指针
        numBytesToRead -= numBytesRead;//减少还要读取多少个字节
      }
      
      
      numBytesRead = recordLength - numBytesToRead;//基本上numBytesToRead都是0,因此numBytesRead总共读取了多少字节,应该等于recordLength,因此我觉得这行代码没什么用
      pos += numBytesRead;//偏移位置更改
      if (numBytesRead > 0) {
        dataRead = true;
        if (numBytesRead >= recordLength) {
          if (!isCompressedInput) {
            numRecordsRemainingInSplit--;//减少一个数据
          }
        } else {
          throw new IOException("Partial record(length = " + numBytesRead
              + ") found at the end of split.");
        }
      } else {
        numRecordsRemainingInSplit = 0L; // End of input.
      }
    }
    return dataRead;
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public BytesWritable getCurrentValue() {
    return value;
  }

  @Override
  public synchronized float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
    }
  }
  
  @Override
  public synchronized void close() throws IOException {
    try {
      if (inputStream != null) {
        inputStream.close();
        inputStream = null;
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
    }
  }

  // This is called from the old FixedLengthRecordReader API implementation.
  public long getPos() {
    return pos;
  }

  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      retVal = pos;
    }
    return retVal;
  }

}
