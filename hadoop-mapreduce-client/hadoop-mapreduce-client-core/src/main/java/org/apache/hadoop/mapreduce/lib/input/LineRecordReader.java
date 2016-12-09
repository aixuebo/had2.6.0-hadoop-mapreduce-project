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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * Treats keys as offset in file and value as line. 
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public class LineRecordReader extends RecordReader<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(LineRecordReader.class);
  public static final String MAX_LINE_LENGTH = "mapreduce.input.linerecordreader.line.maxlength";//行的最大字节数 
  private int maxLineLength;//每行最多允许处理的字节数,即截断一部分字符串,返回截断后的数据给value
  
  private long start;//待处理的数据块在整个文件中的偏移量
  private long pos;//目前已经处理到文件的偏移量位置
  private long end;//待处理的数据块最终到在整个文件中的偏移量位置结束
  
  private FSDataInputStream fileIn;//文件输入流
  private SplitLineReader in;
  private Seekable filePosition;
  
  private LongWritable key;
  private Text value;
  private boolean isCompressedInput;//是否是压缩文件
  private Decompressor decompressor;//解压缩类
  private byte[] recordDelimiterBytes;//以什么结束一行,即一行的分隔符

  public LineRecordReader() {
  }

  public LineRecordReader(byte[] recordDelimiter) {
    this.recordDelimiterBytes = recordDelimiter;
  }

  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
    start = split.getStart();//开始字节位置
    end = start + split.getLength();//结束字节位置
    final Path file = split.getPath();

    // open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(file);//打开文件
    
    CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
    if (null!=codec) {
      isCompressedInput = true;	
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        final SplitCompressionInputStream cIn = ((SplittableCompressionCodec)codec).createInputStream(fileIn, decompressor, start, end,SplittableCompressionCodec.READ_MODE.BYBLOCK);
        in = new CompressedSplitLineReader(cIn, job,this.recordDelimiterBytes);
        start = cIn.getAdjustedStart();
        end = cIn.getAdjustedEnd();
        filePosition = cIn;
      } else {
        in = new SplitLineReader(codec.createInputStream(fileIn,decompressor), job, this.recordDelimiterBytes);
        filePosition = fileIn;
      }
    } else {
      fileIn.seek(start);
      in = new SplitLineReader(fileIn, job, this.recordDelimiterBytes);
      filePosition = fileIn;
    }
    // If this is not the first split, we always throw away first record
    // because we always (except the last split) read one extra line in
    // next() method.
    if (start != 0) {//过滤第一行,因为第一行可能是半个行内容,已经被上一个split处理了数据,本次不需要处理
      start += in.readLine(new Text(), 0, maxBytesToConsume(start));
    }
    this.pos = start;
  }
  

  //返回本次读取做大字节数
  private int maxBytesToConsume(long pos) {
    //如果是压缩的,那就是int就是最大值
    //如果不是压缩的,则最大值就是end-pos,即剩余字节数,如果剩余字节数量<maxLineLength,则返回maxLineLength
    return isCompressedInput
      ? Integer.MAX_VALUE
      : (int) Math.max(Math.min(Integer.MAX_VALUE, end - pos), maxLineLength);
  }

  //当前文件读取的字节位置
  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      retVal = pos;
    }
    return retVal;
  }

  //用于第一个数据块,检查前三个字符是否是UTF-8的编码字符
  private int skipUtfByteOrderMark() throws IOException {
    // Strip BOM(Byte Order Mark)
    // Text only support UTF-8, we only need to check UTF-8 BOM
    // (0xEF,0xBB,0xBF) at the start of the text stream.
    //Text目前只支持UTF-8类型的编码,因此我们要校验UTF8的前三个字节
    int newMaxLineLength = (int) Math.min(3L + (long) maxLineLength,
        Integer.MAX_VALUE);
    int newSize = in.readLine(value, newMaxLineLength, maxBytesToConsume(pos));
    // Even we read 3 extra bytes for the first line,
    // we won't alter existing behavior (no backwards incompat issue).
    // Because the newSize is less than maxLineLength and
    // the number of bytes copied to Text is always no more than newSize.
    // If the return size from readLine is not less than maxLineLength,
    // we will discard the current line and read the next line.
    pos += newSize;
    int textLength = value.getLength();
    byte[] textBytes = value.getBytes();
    if ((textLength >= 3) && (textBytes[0] == (byte)0xEF) &&
        (textBytes[1] == (byte)0xBB) && (textBytes[2] == (byte)0xBF)) {
      // find UTF-8 BOM, strip it.
      LOG.info("Found UTF-8 BOM and skipped it");
      textLength -= 3;
      newSize -= 3;
      if (textLength > 0) {
        // It may work to use the same buffer and not do the copyBytes
        textBytes = value.copyBytes();
        value.set(textBytes, 3, textLength);//重新设置value,除去前三个字节
      } else {
        value.clear();
      }
    }
    return newSize;
  }

  /**
   * 重新设置key和value的值
   * 如果返回false,说明没有文件可以读了,如果返回true,说明key和value设置完成
   */
  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    key.set(pos);//目前已经处理到文件的偏移量位置 作为key
    if (value == null) {
      value = new Text();
    }
    int newSize = 0;
    // We always read one extra line, which lies outside the upper
    // split limit i.e. (end - 1)
    while (getFilePosition() <= end || in.needAdditionalRecordAfterSplit()) {
      if (pos == 0) {//说明是第一个数据块
        newSize = skipUtfByteOrderMark();
      } else {
        newSize = in.readLine(value, maxLineLength, maxBytesToConsume(pos));//从输入流中读一行数据,存储到value中,返回真正读取了多少个字节
        pos += newSize;
      }

      if ((newSize == 0) || (newSize < maxLineLength)) {//如果超出字节限制,则不会break,继续读取下一行数据
        break;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + 
               (pos - newSize));
    }
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
      }
    }
  }
}
