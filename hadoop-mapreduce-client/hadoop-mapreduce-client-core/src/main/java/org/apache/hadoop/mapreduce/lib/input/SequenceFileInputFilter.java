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
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A class that allows a map/red job to work on a sample of sequence files.
 * The sample is decided by the filter class set by the job.
 * 该类就是读取序列化文件,也是读取了全部的数据,只是不是所有的数据都要被处理,只处理filter过滤成true的记录才会被map处理
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileInputFilter<K, V>
    extends SequenceFileInputFormat<K, V> {
  public static final Log LOG = LogFactory.getLog(FileInputFormat.class);
  
  final public static String FILTER_CLASS = 
    "mapreduce.input.sequencefileinputfilter.class";//真正的过滤器类
  final public static String FILTER_FREQUENCY = 
    "mapreduce.input.sequencefileinputfilter.frequency";//基于频率过滤,每次达到这个频率的时候,就返回一次true
  final public static String FILTER_REGEX = 
    "mapreduce.input.sequencefileinputfilter.regex";//基于正则表达式进行过滤时候,输入正则表达式的key,通过该key可以得到对应的正则表达式
    
  public SequenceFileInputFilter() {
  }
    
  /** Create a record reader for the given split
   * @param split file split
   * @param context the task-attempt context
   * @return RecordReader
   */
  public RecordReader<K, V> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    context.setStatus(split.toString());
    return new FilterRecordReader<K, V>(context.getConfiguration());
  }


  /** set the filter class
   * 
   * @param job The job
   * @param filterClass filter class
   */
  public static void setFilterClass(Job job, Class<?> filterClass) {
    job.getConfiguration().set(FILTER_CLASS, filterClass.getName());
  }

         
  /**
   * filter interface
   */
  public interface Filter extends Configurable {
    /** filter function
     * Decide if a record should be filtered or not
     * @param key record key
     * @return true if a record is accepted; return false otherwise
     */
    public abstract boolean accept(Object key);
  }
    
  /**
   * base class for Filters
   */
  public static abstract class FilterBase implements Filter {
    Configuration conf;
        
    public Configuration getConf() {
      return conf;
    }
  }
    
  /** Records filter by matching key to regex
   * 基于正则表达式去过滤
   */
  public static class RegexFilter extends FilterBase {
    private Pattern p;//正则表达式
    /** Define the filtering regex and stores it in conf
     * @param conf where the regex is set
     * @param regex regex used as a filter
     */
    public static void setPattern(Configuration conf, String regex)
        throws PatternSyntaxException {
      try {
        Pattern.compile(regex);
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException("Invalid pattern: "+regex);
      }
      conf.set(FILTER_REGEX, regex);
    }
        
    public RegexFilter() { }
        
    /** configure the Filter by checking the configuration
     */
    public void setConf(Configuration conf) {
      String regex = conf.get(FILTER_REGEX);//获取正则表达式
      if (regex == null)
        throw new RuntimeException(FILTER_REGEX + "not set");
      this.p = Pattern.compile(regex);
      this.conf = conf;
    }


    /** Filtering method
     * If key matches the regex, return true; otherwise return false
     * @see Filter#accept(Object)
     * 判断是否符合正则表达式
     */
    public boolean accept(Object key) {
      return p.matcher(key.toString()).matches();
    }
  }

  /** This class returns a percentage of records
   * The percentage is determined by a filtering frequency <i>f</i> using
   * the criteria record# % f == 0.
   * For example, if the frequency is 10, one out of 10 records is returned.
   */
  public static class PercentFilter extends FilterBase {
    private int frequency;//基于频率过滤,每次达到这个频率的时候,就返回一次true
    private int count;

    /** set the frequency and stores it in conf
     * @param conf configuration
     * @param frequency filtering frequencey
     */
    public static void setFrequency(Configuration conf, int frequency) {
      if (frequency <= 0)
        throw new IllegalArgumentException(
          "Negative " + FILTER_FREQUENCY + ": " + frequency);
      conf.setInt(FILTER_FREQUENCY, frequency);
    }
        
    public PercentFilter() { }
        
    /** configure the filter by checking the configuration
     * 
     * @param conf configuration
     */
    public void setConf(Configuration conf) {
      this.frequency = conf.getInt(FILTER_FREQUENCY, 10);
      if (this.frequency <= 0) {
        throw new RuntimeException(
          "Negative "+FILTER_FREQUENCY + ": " + this.frequency);
      }
      this.conf = conf;
    }

    /** Filtering method
     * If record# % frequency==0, return true; otherwise return false
     * @see Filter#accept(Object)
     */
    public boolean accept(Object key) {
      boolean accepted = false;
      if (count == 0)
        accepted = true;
      if (++count == frequency) {//基于频率过滤,每次达到这个频率的时候,就返回一次true
        count = 0;
      }
      return accepted;
    }
  }

  /** This class returns a set of records by examing the MD5 digest of its
   * key against a filtering frequency <i>f</i>. The filtering criteria is
   * MD5(key) % f == 0.
   */
  public static class MD5Filter extends FilterBase {
    private int frequency;
    private static final MessageDigest DIGESTER;//加密算法
    public static final int MD5_LEN = 16;
    private byte [] digest = new byte[MD5_LEN];//最终用于存储MD5的数据结果
        
    static {
      try {
        DIGESTER = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }


    /** set the filtering frequency in configuration
     * 
     * @param conf configuration
     * @param frequency filtering frequency
     */
    public static void setFrequency(Configuration conf, int frequency) {
      if (frequency <= 0)
        throw new IllegalArgumentException(
          "Negative " + FILTER_FREQUENCY + ": " + frequency);
      conf.setInt(FILTER_FREQUENCY, frequency);
    }
        
    public MD5Filter() { }
        
    /** configure the filter according to configuration
     * 
     * @param conf configuration
     */
    public void setConf(Configuration conf) {
      this.frequency = conf.getInt(FILTER_FREQUENCY, 10);
      if (this.frequency <= 0) {
        throw new RuntimeException(
          "Negative " + FILTER_FREQUENCY + ": " + this.frequency);
      }
      this.conf = conf;
    }

    /** Filtering method
     * If MD5(key) % frequency==0, return true; otherwise return false
     * @see Filter#accept(Object)
     */
    public boolean accept(Object key) {
      try {
        long hashcode;
        if (key instanceof Text) {
          hashcode = MD5Hashcode((Text)key);
        } else if (key instanceof BytesWritable) {
          hashcode = MD5Hashcode((BytesWritable)key);
        } else {
          ByteBuffer bb;
          bb = Text.encode(key.toString());
          hashcode = MD5Hashcode(bb.array(), 0, bb.limit());
        }
        if (hashcode / frequency * frequency == hashcode) //表示hashcode 正好被frequency整除的时候,才会结果依然是hashcode,即只有MD5(key) % frequency==0 整除,没有余数的时候,才会算该值
          return true;
      } catch(Exception e) {
        LOG.warn(e);
        throw new RuntimeException(e);
      }
      return false;
    }
        
    private long MD5Hashcode(Text key) throws DigestException {
      return MD5Hashcode(key.getBytes(), 0, key.getLength());
    }
        
    private long MD5Hashcode(BytesWritable key) throws DigestException {
      return MD5Hashcode(key.getBytes(), 0, key.getLength());
    }
    
    //对字节数组进行MD5设置,然后再进行hash,返回一个long值
    synchronized private long MD5Hashcode(byte[] bytes, 
        int start, int length) throws DigestException {
      DIGESTER.update(bytes, 0, length);
      DIGESTER.digest(digest, 0, MD5_LEN);//生成MD5数据
      long hashcode=0;
      for (int i = 0; i < 8; i++)
        hashcode |= ((digest[i] & 0xffL) << (8 * (7 - i)));
      return hashcode;
    }
  }
    
  private static class FilterRecordReader<K, V>
      extends SequenceFileRecordReader<K, V> {
    
    private Filter filter;
    private K key;
    private V value;
        
    public FilterRecordReader(Configuration conf)
        throws IOException {
      super();
      // instantiate filter
      filter = (Filter)ReflectionUtils.newInstance(
        conf.getClass(FILTER_CLASS, PercentFilter.class), conf);
    }
    
    public synchronized boolean nextKeyValue() 
        throws IOException, InterruptedException {
      while (super.nextKeyValue()) {
        key = super.getCurrentKey();
        if (filter.accept(key)) {//只有过滤器通过的数据才会被处理,虽然也是加载了所有的数据,但是只是符合过滤器通过的数据才会被map处理成一条记录
          value = super.getCurrentValue();
          return true;
        }
      }
      return false;
    }
    
    @Override
    public K getCurrentKey() {
      return key;
    }
    
    @Override
    public V getCurrentValue() {
      return value;
    }
  }
}
