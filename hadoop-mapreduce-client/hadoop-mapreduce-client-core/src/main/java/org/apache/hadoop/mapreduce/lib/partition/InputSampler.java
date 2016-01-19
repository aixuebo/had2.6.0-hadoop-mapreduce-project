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

package org.apache.hadoop.mapreduce.lib.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * Utility for collecting samples and writing a partition file for
 * {@link TotalOrderPartitioner}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InputSampler<K,V> extends Configured implements Tool  {

  private static final Log LOG = LogFactory.getLog(InputSampler.class);

  /**
   * 1.-r reduce数量
   * 2.-inFormat <input format class> 设置job.setInputFormatClass,该类必须是InputFormat的子类
   * 3.-keyClass 设置job.setMapOutputKeyClass,该类必须是WritableComparable的子类
   * 4.-splitSample <numSamples> <maxsplits> 创建SplitSampler对象
   * 5.-splitRandom <double pcnt> <numSamples> <maxsplits> 创建RandomSampler对象
   * 6.-splitInterval <double pcnt> <maxsplits> 创建IntervalSampler对象
   */
  static int printUsage() {
    System.out.println("sampler -r <reduces>\n" +
      "      [-inFormat <input format class>]\n" +
      "      [-keyClass <map input & output key class>]\n" +
      "      [-splitRandom <double pcnt> <numSamples> <maxsplits> | " +
      "             // Sample from random splits at random (general)\n" +
      "       -splitSample <numSamples> <maxsplits> | " +
      "             // Sample from first records in splits (random data)\n"+
      "       -splitInterval <double pcnt> <maxsplits>]" +
      "             // Sample from splits at intervals (sorted data)");
    System.out.println("Default sampler: -splitRandom 0.1 10000 10");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  public InputSampler(Configuration conf) {
    setConf(conf);
  }

  /**
   * Interface to sample using an 
   * {@link org.apache.hadoop.mapreduce.InputFormat}.
   */
  public interface Sampler<K,V> {
    /**
     * For a given job, collect and return a subset of the keys from the
     * input data.
     */
    K[] getSample(InputFormat<K,V> inf, Job job) 
    throws IOException, InterruptedException;
  }

  /**
   * Samples the first n records from s splits.
   * Inexpensive way to sample random data.
   * 每个split数据源文件 顺序提取前top个记录作为数据集合被采取
   */
  public static class SplitSampler<K,V> implements Sampler<K,V> {

    protected final int numSamples;//获取多少个元素
    protected final int maxSplitsSampled;//最多拆分maxSplitsSampled个,如果数据源文件较多,则忽略一部分文件

    /**
     * Create a SplitSampler sampling <em>all</em> splits.
     * Takes the first numSamples / numSplits records from each split.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     */
    public SplitSampler(int numSamples) {
      this(numSamples, Integer.MAX_VALUE);
    }

    /**
     * Create a new SplitSampler.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     * @param maxSplitsSampled The maximum number of splits to examine.
     */
    public SplitSampler(int numSamples, int maxSplitsSampled) {
      this.numSamples = numSamples;
      this.maxSplitsSampled = maxSplitsSampled;
    }

    /**
     * From each split sampled, take the first numSamples / numSplits records.
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, Job job) 
        throws IOException, InterruptedException {
      List<InputSplit> splits = inf.getSplits(job);
      ArrayList<K> samples = new ArrayList<K>(numSamples);
      int splitsToSample = Math.min(maxSplitsSampled, splits.size());//一共要提取多少个split文件作为数据源
      int samplesPerSplit = numSamples / splitsToSample;//每个split提取多少个元素
      long records = 0;
      for (int i = 0; i < splitsToSample; ++i) {
        TaskAttemptContext samplingContext = new TaskAttemptContextImpl(
            job.getConfiguration(), new TaskAttemptID());
        RecordReader<K,V> reader = inf.createRecordReader(
            splits.get(i), samplingContext);
        reader.initialize(splits.get(i), samplingContext);
        while (reader.nextKeyValue()) {
          samples.add(ReflectionUtils.copy(job.getConfiguration(),
                                           reader.getCurrentKey(), null));
          ++records;
          if ((i+1) * samplesPerSplit <= records) {
            break;
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

  /**
   * Sample from random points in the input.
   * General-purpose sampler. Takes numSamples / maxSplitsSampled inputs from
   * each split.
   * 随机提取器
   */
  public static class RandomSampler<K,V> implements Sampler<K,V> {
    protected double freq;//百分比,录入该值等于0.3,表示30%数据将会被提取出来
    protected final int numSamples;//获取多少个元素
    protected final int maxSplitsSampled;//最多拆分maxSplitsSampled个,如果数据源文件较多,则忽略一部分文件

    /**
     * Create a new RandomSampler sampling <em>all</em> splits.
     * This will read every split at the client, which is very expensive.
     * @param freq Probability with which a key will be chosen.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     */
    public RandomSampler(double freq, int numSamples) {
      this(freq, numSamples, Integer.MAX_VALUE);
    }

    /**
     * Create a new RandomSampler.
     * @param freq Probability with which a key will be chosen.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     * @param maxSplitsSampled The maximum number of splits to examine.
     */
    public RandomSampler(double freq, int numSamples, int maxSplitsSampled) {
      this.freq = freq;
      this.numSamples = numSamples;
      this.maxSplitsSampled = maxSplitsSampled;
    }

    /**
     * Randomize the split order, then take the specified number of keys from
     * each split sampled, where each key is selected with the specified
     * probability and possibly replaced by a subsequently selected key when
     * the quota of keys from that split is satisfied.
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, Job job) 
        throws IOException, InterruptedException {
      List<InputSplit> splits = inf.getSplits(job);
      ArrayList<K> samples = new ArrayList<K>(numSamples);
      int splitsToSample = Math.min(maxSplitsSampled, splits.size());

      Random r = new Random();
      long seed = r.nextLong();
      r.setSeed(seed);
      LOG.debug("seed: " + seed);
      // shuffle splits 将split进行重新洗牌,洗乱
      for (int i = 0; i < splits.size(); ++i) {
        InputSplit tmp = splits.get(i);//i位置元素
        int j = r.nextInt(splits.size());//随机找到一个位置j
        splits.set(i, splits.get(j));//将j位置元素设置到i位置
        splits.set(j, tmp);//将i位置元素设置到j位置
      }
      // our target rate is in terms of the maximum number of sample splits,
      // but we accept the possibility of sampling additional splits to hit
      // the target sample keyset 读取splitsToSample个split文件作为数据源提取数据,由于该数据源已经经过重新洗牌了,所以已经是无序了
      for (int i = 0; i < splitsToSample ||
                     (i < splits.size() && samples.size() < numSamples); ++i) {
        TaskAttemptContext samplingContext = new TaskAttemptContextImpl(
            job.getConfiguration(), new TaskAttemptID());
        RecordReader<K,V> reader = inf.createRecordReader(
            splits.get(i), samplingContext);
        reader.initialize(splits.get(i), samplingContext);
        while (reader.nextKeyValue()) {
          if (r.nextDouble() <= freq) {//如果小于随机概率,则向数据集合中添加该数据
            if (samples.size() < numSamples) {//向数据集合中添加该数据
              samples.add(ReflectionUtils.copy(job.getConfiguration(),
                                               reader.getCurrentKey(), null));
            } else {
              // When exceeding the maximum number of samples, replace a
              // random element with this one, then adjust the frequency
              // to reflect the possibility of existing elements being
              // pushed out 当提取的元素超过了最大的元素数量,则降低概率
              int ind = r.nextInt(numSamples);
              if (ind != numSamples) {
                samples.set(ind, ReflectionUtils.copy(job.getConfiguration(),
                                 reader.getCurrentKey(), null));
              }
              freq *= (numSamples - 1) / (double) numSamples;
            }
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

  /**
   * Sample from s splits at regular intervals.
   * Useful for sorted data.
   * 按照百分比寻找数据
   */
  public static class IntervalSampler<K,V> implements Sampler<K,V> {
    protected final double freq;//百分比,录入该值等于0.3,表示30%数据将会被提取出来
    protected final int maxSplitsSampled;//最多拆分maxSplitsSampled个,如果数据源文件较多,则忽略一部分文件

    /**
     * Create a new IntervalSampler sampling <em>all</em> splits.
     * @param freq The frequency with which records will be emitted.
     */
    public IntervalSampler(double freq) {
      this(freq, Integer.MAX_VALUE);
    }

    /**
     * Create a new IntervalSampler.
     * @param freq The frequency with which records will be emitted.
     * @param maxSplitsSampled The maximum number of splits to examine.
     * @see #getSample
     */
    public IntervalSampler(double freq, int maxSplitsSampled) {
      this.freq = freq;
      this.maxSplitsSampled = maxSplitsSampled;
    }

    /**
     * For each split sampled, emit when the ratio of the number of records
     * retained to the total record count is less than the specified
     * frequency.
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, Job job) 
        throws IOException, InterruptedException {
      List<InputSplit> splits = inf.getSplits(job);
      ArrayList<K> samples = new ArrayList<K>();
      int splitsToSample = Math.min(maxSplitsSampled, splits.size());//最多拆分maxSplitsSampled个,如果数据源文件较多,则忽略一部分文件
      long records = 0;//记录数
      long kept = 0;
      for (int i = 0; i < splitsToSample; ++i) {
        TaskAttemptContext samplingContext = new TaskAttemptContextImpl(
            job.getConfiguration(), new TaskAttemptID());
        RecordReader<K,V> reader = inf.createRecordReader(
            splits.get(i), samplingContext);
        reader.initialize(splits.get(i), samplingContext);
        while (reader.nextKeyValue()) {
          ++records;
          if ((double) kept / records < freq) {//符合条件的数/总数 <百分比,就提取出来,说明就是百分比
            samples.add(ReflectionUtils.copy(job.getConfiguration(),reader.getCurrentKey(), null));
            ++kept;
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
    
    /**
     * maming 测试百分比对象
     */
    @Test
    public void test1(){
      long records = 0;
      long kept = 0;
      double freq = 0.3;
      
      int count = 0;
      
      for(int i=0;i<100;i++) {
        ++records;
        System.out.println((double) kept / records);
        if ((double) kept / records < freq) {
          System.out.println("----"+i);
          ++kept;
          count++;
        }
      }
      
      System.out.println("count:"+count);
      
    }
  }

  /**
   * Write a partition file for the given job, using the Sampler provided.
   * Queries the sampler for a sample keyset, sorts by the output key
   * comparator, selects the keys for each rank, and writes to the destination
   * returned from {@link TotalOrderPartitioner#getPartitionFile}.
   */
  @SuppressWarnings("unchecked") // getInputFormat, getOutputKeyComparator
  public static <K,V> void writePartitionFile(Job job, Sampler<K,V> sampler) 
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = job.getConfiguration();
    final InputFormat inf = 
        ReflectionUtils.newInstance(job.getInputFormatClass(), conf);
    int numPartitions = job.getNumReduceTasks();
    //进行抽样结果提取
    K[] samples = (K[])sampler.getSample(inf, job);
    LOG.info("Using " + samples.length + " samples");
    RawComparator<K> comparator =
      (RawComparator<K>) job.getSortComparator();
    Arrays.sort(samples, comparator);//对抽样结果进行排序
    
    //生成split拆分文件
    Path dst = new Path(TotalOrderPartitioner.getPartitionFile(conf));
    FileSystem fs = dst.getFileSystem(conf);
    if (fs.exists(dst)) {
      fs.delete(dst, false);
    }
    
    //每隔一定数量添加一个数据到split文件中
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, 
      conf, dst, job.getMapOutputKeyClass(), NullWritable.class);
    NullWritable nullValue = NullWritable.get();
    float stepSize = samples.length / (float) numPartitions;
    int last = -1;
    for(int i = 1; i < numPartitions; ++i) {
      int k = Math.round(stepSize * i);
      while (last >= k && comparator.compare(samples[last], samples[k]) == 0) {//确保两个添加的任务不是一个split分区的
        ++k;
      }
      writer.append(samples[k], nullValue);
      last = k;
    }
    writer.close();
  }

  /**
   * Driver for InputSampler from the command line.
   * Configures a JobConf instance and calls {@link #writePartitionFile}.
   * 参数集合是各种参数解析，以及input输入源、key的split拆分文件
   */
  public int run(String[] args) throws Exception {
    Job job = new Job(getConf());
    //存放其他参数
    ArrayList<String> otherArgs = new ArrayList<String>();
    Sampler<K,V> sampler = null;
    for(int i=0; i < args.length; ++i) {
      try {
        //解析参数和其他参数
        if ("-r".equals(args[i])) {
          job.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else if ("-inFormat".equals(args[i])) {
          job.setInputFormatClass(
              Class.forName(args[++i]).asSubclass(InputFormat.class));
        } else if ("-keyClass".equals(args[i])) {
          job.setMapOutputKeyClass(
              Class.forName(args[++i]).asSubclass(WritableComparable.class));
        } else if ("-splitSample".equals(args[i])) {
          int numSamples = Integer.parseInt(args[++i]);
          int maxSplits = Integer.parseInt(args[++i]);
          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
          sampler = new SplitSampler<K,V>(numSamples, maxSplits);
        } else if ("-splitRandom".equals(args[i])) {
          double pcnt = Double.parseDouble(args[++i]);
          int numSamples = Integer.parseInt(args[++i]);
          int maxSplits = Integer.parseInt(args[++i]);
          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
          sampler = new RandomSampler<K,V>(pcnt, numSamples, maxSplits);
        } else if ("-splitInterval".equals(args[i])) {
          double pcnt = Double.parseDouble(args[++i]);
          int maxSplits = Integer.parseInt(args[++i]);
          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
          sampler = new IntervalSampler<K,V>(pcnt, maxSplits);
        } else {
          otherArgs.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
            args[i-1]);
        return printUsage();
      }
    }
    
    //参数校验
    if (job.getNumReduceTasks() <= 1) {
      System.err.println("Sampler requires more than one reducer");
      return printUsage();
    }
    if (otherArgs.size() < 2) {
      System.out.println("ERROR: Wrong number of parameters: ");
      return printUsage();
    }
    if (null == sampler) {
      sampler = new RandomSampler<K,V>(0.1, 10000, 10);
    }

    //最后一个参数是TotalOrderPartitioner类使用的key的拆分文件
    Path outf = new Path(otherArgs.remove(otherArgs.size() - 1));
    TotalOrderPartitioner.setPartitionFile(getConf(), outf);
    
    //将其他参数输入路径,将其添加到参数里面
    for (String s : otherArgs) {
      FileInputFormat.addInputPath(job, new Path(s));
    }
    
    //运行job
    InputSampler.<K,V>writePartitionFile(job, sampler);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    InputSampler<?,?> sampler = new InputSampler(new Configuration());
    int res = ToolRunner.run(sampler, args);
    System.exit(res);
  }
}
