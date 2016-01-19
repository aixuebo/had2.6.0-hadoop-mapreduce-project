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
package org.apache.hadoop.mapreduce.lib.chain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Stringifier;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The Chain class provides all the common functionality for the
 * {@link ChainMapper} and the {@link ChainReducer} classes.
 * 
该链条逻辑留存:
设置mapper对象到配置文件中
   * 1.校验
   * 2.获取该map的索引顺序
   * 3.在主job配置文件中设置key为前缀_map_索引 value是map对应的实现类
   * 4.校验输入是否是上一个的输出
   * 5.设置该map私有的配置文件,将该map的输入输出key-value设置到私有的配置中
   * 6.强私有的map配置添加到主配置文件中
   * 7.累加Map的索引数量+1
  protected static void addMapper(boolean isMap, Job job,
      Class<? extends Mapper> klass, Class<?> inputKeyClass,
      Class<?> inputValueClass, Class<?> outputKeyClass,
      Class<?> outputValueClass, Configuration mapperConf) {

设置reduce对象到配置文件中
   *1.校验
   *2.在主job配置中设置reduce的class
   *3.在reduce自己的配置文件中配置输入输出的key-value对象
   *4.将reduce自己的配置文件写入主job的配置文件中        
  protected static void setReducer(Job job, Class<? extends Reducer> klass,
      Class<?> inputKeyClass, Class<?> inputValueClass,
      Class<?> outputKeyClass, Class<?> outputValueClass,
      Configuration reducerConf) {


setup方法调用,初始化所有配置的Map对象和reduce对象

实例化Map和reduce,添加到Thread线程集合中


  // start all the threads 线程集合一个接一个的去执行
  void startAllThreads() {
    for (Thread thread : threads) {
      thread.start();
    }
  }


 * 多个map-reduce连续执行
 * 注意:
 * 1.reduce必须当做第一个入口,然后执行map
 * 2.只能有一个reduce,可以有多个map
 * 比如:
 * 1.reduce-map1 - map2 - map3
 * 2.map1 - map2 - map3
 * 
 * 可以将ChainReducer、ChainMapper组合使用,以达到任何匹配方式
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Chain {
  protected static final String CHAIN_MAPPER = "mapreduce.chain.mapper";
  protected static final String CHAIN_REDUCER = "mapreduce.chain.reducer";

  protected static final String CHAIN_MAPPER_SIZE = ".size";//获取map/reduce排序序号
  protected static final String CHAIN_MAPPER_CLASS = ".mapper.class.";//获取mapper对应的类,与序列号的关系
  protected static final String CHAIN_MAPPER_CONFIG = ".mapper.config.";
  protected static final String CHAIN_REDUCER_CLASS = ".reducer.class";//获取reduce对应的类
  protected static final String CHAIN_REDUCER_CONFIG = ".reducer.config";

  protected static final String MAPPER_INPUT_KEY_CLASS = 
    "mapreduce.chain.mapper.input.key.class";//map的key的输入对象
  protected static final String MAPPER_INPUT_VALUE_CLASS = 
    "mapreduce.chain.mapper.input.value.class";//map的value的输入对象
  protected static final String MAPPER_OUTPUT_KEY_CLASS = 
    "mapreduce.chain.mapper.output.key.class";//map的key的输出对象
  protected static final String MAPPER_OUTPUT_VALUE_CLASS = 
    "mapreduce.chain.mapper.output.value.class";//map的value的输出对象
  protected static final String REDUCER_INPUT_KEY_CLASS = 
    "mapreduce.chain.reducer.input.key.class";//reduce的key的输入对象
  protected static final String REDUCER_INPUT_VALUE_CLASS = 
    "maperduce.chain.reducer.input.value.class";//reduce的value的输入对象
  protected static final String REDUCER_OUTPUT_KEY_CLASS = 
    "mapreduce.chain.reducer.output.key.class";//reduce的key的输出对象
  protected static final String REDUCER_OUTPUT_VALUE_CLASS = 
    "mapreduce.chain.reducer.output.value.class";//reduce的value的输出对象

  protected boolean isMap;//true表示map任务,value表示reduce任务

  @SuppressWarnings("unchecked")
  private List<Mapper> mappers = new ArrayList<Mapper>();//存储所有mapper对象
  private Reducer<?, ?, ?, ?> reducer;//存储reduce对象
  private List<Configuration> confList = new ArrayList<Configuration>();//存储每一个Mapper任务的配置文件集合
  private Configuration rConf;
  //运行Mapper的线程池集合
  private List<Thread> threads = new ArrayList<Thread>();
  private List<ChainBlockingQueue<?>> blockingQueues = 
    new ArrayList<ChainBlockingQueue<?>>();
  private Throwable throwable = null;//异常对象

  /**
   * Creates a Chain instance configured for a Mapper or a Reducer.
   * 
   * @param isMap
   *          TRUE indicates the chain is for a Mapper, FALSE that is for a
   *          Reducer.
   */
  protected Chain(boolean isMap) {
    this.isMap = isMap;
  }

  static class KeyValuePair<K, V> {
    K key;
    V value;
    boolean endOfInput;

    KeyValuePair(K key, V value) {
      this.key = key;
      this.value = value;
      this.endOfInput = false;
    }

    KeyValuePair(boolean eof) {
      this.key = null;
      this.value = null;
      this.endOfInput = eof;
    }
  }

  // ChainRecordReader either reads from blocking queue or task context.读数据对象
  //详细信息参见ChainRecordWriter类中如何写入数据
  private static class ChainRecordReader<KEYIN, VALUEIN> extends
      RecordReader<KEYIN, VALUEIN> {
    private Class<?> keyClass;
    private Class<?> valueClass;
    private KEYIN key;
    private VALUEIN value;
    private Configuration conf;
    TaskInputOutputContext<KEYIN, VALUEIN, ?, ?> inputContext = null;
    ChainBlockingQueue<KeyValuePair<KEYIN, VALUEIN>> inputQueue = null;

    // constructor to read from a blocking queue
    ChainRecordReader(Class<?> keyClass, Class<?> valueClass,
        ChainBlockingQueue<KeyValuePair<KEYIN, VALUEIN>> inputQueue,
        Configuration conf) {
      this.keyClass = keyClass;
      this.valueClass = valueClass;
      this.inputQueue = inputQueue;
      this.conf = conf;
    }

    // constructor to read from the context
    ChainRecordReader(TaskInputOutputContext<KEYIN, VALUEIN, ?, ?> context) {
      inputContext = context;
    }

    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    /**
     * Advance to the next key, value pair, returning null if at end.
     * 
     * @return the key object that was read into, or null if no more
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (inputQueue != null) {//从队列中获取数据
        return readFromQueue();
      } else if (inputContext.nextKeyValue()) {//从上下文中获取数据
        this.key = inputContext.getCurrentKey();
        this.value = inputContext.getCurrentValue();
        return true;
      } else {
        return false;
      }
    }

    @SuppressWarnings("unchecked")
    private boolean readFromQueue() throws IOException, InterruptedException {
      KeyValuePair<KEYIN, VALUEIN> kv = null;

      // wait for input on queue
      kv = inputQueue.dequeue();
      if (kv.endOfInput) {
        return false;
      }
      //实例化key-value对象
      key = (KEYIN) ReflectionUtils.newInstance(keyClass, conf);
      value = (VALUEIN) ReflectionUtils.newInstance(valueClass, conf);
      //将kv.key的信息复制到this.key中
      ReflectionUtils.copy(conf, kv.key, this.key);
      //将kv.value的信息复制到this.value中
      ReflectionUtils.copy(conf, kv.value, this.value);
      return true;
    }

    /**
     * Get the current key.
     * 
     * @return the current key object or null if there isn't one
     * @throws IOException
     * @throws InterruptedException
     */
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
      return this.key;
    }

    /**
     * Get the current value.
     * 
     * @return the value object that was read into
     * @throws IOException
     * @throws InterruptedException
     */
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
      return this.value;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }
  }

  // ChainRecordWriter either writes to blocking queue or task context 写数据对象
  private static class ChainRecordWriter<KEYOUT, VALUEOUT> extends
      RecordWriter<KEYOUT, VALUEOUT> {
    TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> outputContext = null;//向数据源写数据
    ChainBlockingQueue<KeyValuePair<KEYOUT, VALUEOUT>> outputQueue = null;//向队列写数据
    KEYOUT keyout;//写入的key的实例
    VALUEOUT valueout;//写入的value的实例
    Configuration conf;
    Class<?> keyClass;//写入的key类
    Class<?> valueClass;//写入的value类

    // constructor to write to context 向数据源写入数据
    ChainRecordWriter(TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context) {
      outputContext = context;
    }

    // constructor to write to blocking queue 向队列写入数据
    ChainRecordWriter(Class<?> keyClass, Class<?> valueClass,
        ChainBlockingQueue<KeyValuePair<KEYOUT, VALUEOUT>> output,
        Configuration conf) {
      this.keyClass = keyClass;
      this.valueClass = valueClass;
      this.outputQueue = output;
      this.conf = conf;
    }

    /**
     * Writes a key/value pair.
     * 
     * @param key
     *          the key to write.
     * @param value
     *          the value to write.
     * @throws IOException
     */
    public void write(KEYOUT key, VALUEOUT value) throws IOException,
        InterruptedException {
      if (outputQueue != null) {//向队列写入数据
        writeToQueue(key, value);
      } else {//向输出源写入数据
        outputContext.write(key, value);
      }
    }

    //向队列写入数据,参数是含有具体内容的数据
    @SuppressWarnings("unchecked")
    private void writeToQueue(KEYOUT key, VALUEOUT value) throws IOException,
        InterruptedException {
      //实例化key和value
      this.keyout = (KEYOUT) ReflectionUtils.newInstance(keyClass, conf);
      this.valueout = (VALUEOUT) ReflectionUtils.newInstance(valueClass, conf);
      //将key和value的值 写入刚刚实例化的对象内
      ReflectionUtils.copy(conf, key, this.keyout);
      ReflectionUtils.copy(conf, value, this.valueout);

      //将key-value添加到队列中
      // wait to write output to queuue
      outputQueue.enqueue(new KeyValuePair<KEYOUT, VALUEOUT>(keyout, valueout));
    }

    /**
     * Close this <code>RecordWriter</code> to future operations.
     * 
     * @param context
     *          the context of the task
     * @throws IOException
     */
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      if (outputQueue != null) {
        // write end of input
        outputQueue.enqueue(new KeyValuePair<KEYOUT, VALUEOUT>(true));
      }
    }

  }

  private synchronized Throwable getThrowable() {
    return throwable;
  }

  //设置异常,前提是异常必须不存在的情况下才允许设置异常
  private synchronized boolean setIfUnsetThrowable(Throwable th) {
    if (throwable == null) {
      throwable = th;
      return true;
    }
    return false;
  }

  private class MapRunner<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Thread {
    private Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper;
    private Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context chainContext;
    private RecordReader<KEYIN, VALUEIN> rr;
    private RecordWriter<KEYOUT, VALUEOUT> rw;

    public MapRunner(Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper,
        Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context mapperContext,
        RecordReader<KEYIN, VALUEIN> rr, RecordWriter<KEYOUT, VALUEOUT> rw)
        throws IOException, InterruptedException {
      this.mapper = mapper;
      this.rr = rr;
      this.rw = rw;
      this.chainContext = mapperContext;
    }

    @Override
    public void run() {
      if (getThrowable() != null) {
        return;
      }
      try {
        //运行每一个Map对象,包括setup,每一个map方法
        mapper.run(chainContext);
        rr.close();
        rw.close(chainContext);
      } catch (Throwable th) {
        if (setIfUnsetThrowable(th)) {
          interruptAllThreads();
        }
      }
    }
  }

  private class ReduceRunner<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Thread {
    private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer;
    private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context chainContext;
    private RecordWriter<KEYOUT, VALUEOUT> rw;

    ReduceRunner(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context,
        Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer,
        RecordWriter<KEYOUT, VALUEOUT> rw) throws IOException,
        InterruptedException {
      this.reducer = reducer;
      this.chainContext = context;
      this.rw = rw;
    }

    @Override
    public void run() {
      try {
        //运行每一个Reduce对象,包括setup,每一个reduce方法
        reducer.run(chainContext);
        rw.close(chainContext);
      } catch (Throwable th) {
        if (setIfUnsetThrowable(th)) {
          interruptAllThreads();
        }
      }
    }
  }

  /**
   * 获取第index个Configuration对象
   */
  Configuration getConf(int index) {
    return confList.get(index);
  }

  /**
   * Create a map context that is based on ChainMapContext and the given record
   * reader and record writer
   * 创建map的上下文对象
   */
  private <KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
  Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context createMapContext(
      RecordReader<KEYIN, VALUEIN> rr, RecordWriter<KEYOUT, VALUEOUT> rw,
      TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context,
      Configuration conf) {
    MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext = 
      new ChainMapContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(
        context, rr, rw, conf);
    Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context mapperContext = 
      new WrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>()
        .getMapContext(mapContext);
    return mapperContext;
  }

  /**
   * 运行下标为index的Map对象
   */
  @SuppressWarnings("unchecked")
  void runMapper(TaskInputOutputContext context, int index) throws IOException,
      InterruptedException {
    Mapper mapper = mappers.get(index);
    RecordReader rr = new ChainRecordReader(context);
    RecordWriter rw = new ChainRecordWriter(context);
    Mapper.Context mapperContext = createMapContext(rr, rw, context,
        getConf(index));
    mapper.run(mapperContext);
    rr.close();
    rw.close(context);
  }

  /**
   * Add mapper(the first mapper) that reads input from the input
   * context and writes to queue
   * 添加一个mapper对象到队列中
   * 从输入流中读取数据,写入队列中
   */
  @SuppressWarnings("unchecked")
  void addMapper(TaskInputOutputContext inputContext,
      ChainBlockingQueue<KeyValuePair<?, ?>> output, int index)
      throws IOException, InterruptedException {
    Configuration conf = getConf(index);//获取该index对应的配置文件
    //找到该map的输出key与value
    Class<?> keyOutClass = conf.getClass(MAPPER_OUTPUT_KEY_CLASS, Object.class);
    Class<?> valueOutClass = conf.getClass(MAPPER_OUTPUT_VALUE_CLASS,
        Object.class);

    //创建读对象和输出对象和上下文对象
    RecordReader rr = new ChainRecordReader(inputContext);
    RecordWriter rw = new ChainRecordWriter(keyOutClass, valueOutClass, output,
        conf);
    Mapper.Context mapperContext = createMapContext(rr, rw,
        (MapContext) inputContext, getConf(index));
    //将Mapper线程对象添加到线程池中进行运行
    MapRunner runner = new MapRunner(mappers.get(index), mapperContext, rr, rw);
    threads.add(runner);
  }

  /**
   * Add mapper(the last mapper) that reads input from
   * queue and writes output to the output context
   * 从队列中读取数据,写入到输出流中
   */
  @SuppressWarnings("unchecked")
  void addMapper(ChainBlockingQueue<KeyValuePair<?, ?>> input,
      TaskInputOutputContext outputContext, int index) throws IOException,
      InterruptedException {
    Configuration conf = getConf(index);
    Class<?> keyClass = conf.getClass(MAPPER_INPUT_KEY_CLASS, Object.class);
    Class<?> valueClass = conf.getClass(MAPPER_INPUT_VALUE_CLASS, Object.class);
    RecordReader rr = new ChainRecordReader(keyClass, valueClass, input, conf);
    RecordWriter rw = new ChainRecordWriter(outputContext);
    MapRunner runner = new MapRunner(mappers.get(index), createMapContext(rr,
        rw, outputContext, getConf(index)), rr, rw);
    threads.add(runner);
  }

  /**
   * Add mapper that reads and writes from/to the queue
   * 从队列中读取数据,写入队列中
   */
  @SuppressWarnings("unchecked")
  void addMapper(ChainBlockingQueue<KeyValuePair<?, ?>> input,
      ChainBlockingQueue<KeyValuePair<?, ?>> output,
      TaskInputOutputContext context, int index) throws IOException,
      InterruptedException {
    Configuration conf = getConf(index);
    Class<?> keyClass = conf.getClass(MAPPER_INPUT_KEY_CLASS, Object.class);
    Class<?> valueClass = conf.getClass(MAPPER_INPUT_VALUE_CLASS, Object.class);
    Class<?> keyOutClass = conf.getClass(MAPPER_OUTPUT_KEY_CLASS, Object.class);
    Class<?> valueOutClass = conf.getClass(MAPPER_OUTPUT_VALUE_CLASS,
        Object.class);
    RecordReader rr = new ChainRecordReader(keyClass, valueClass, input, conf);
    RecordWriter rw = new ChainRecordWriter(keyOutClass, valueOutClass, output,
        conf);
    MapRunner runner = new MapRunner(mappers.get(index), createMapContext(rr,
        rw, context, getConf(index)), rr, rw);
    threads.add(runner);
  }

  /**
   * Create a reduce context that is based on ChainMapContext and the given
   * record writer
   * 创建reduce的上下文
   */
  private <KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
  Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context createReduceContext(
      RecordWriter<KEYOUT, VALUEOUT> rw,
      ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context,
      Configuration conf) {
    ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext = 
      new ChainReduceContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(
          context, rw, conf);
    Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context reducerContext = 
      new WrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>()
        .getReducerContext(reduceContext);
    return reducerContext;
  }

  /**
   * 运行一个reduce
   */
  // Run the reducer directly.
  @SuppressWarnings("unchecked")
  <KEYIN, VALUEIN, KEYOUT, VALUEOUT> void runReducer(
      TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context)
      throws IOException, InterruptedException {
    RecordWriter<KEYOUT, VALUEOUT> rw = new ChainRecordWriter<KEYOUT, VALUEOUT>(
        context);
    Reducer.Context reducerContext = createReduceContext(rw,
        (ReduceContext) context, rConf);
    reducer.run(reducerContext);
    rw.close(context);
  }

  /**
   * Add reducer that reads from context and writes to a queue
   * 添加一个reduce
   * 从输出流中写入队列中
   */
  @SuppressWarnings("unchecked")
  void addReducer(TaskInputOutputContext inputContext,
      ChainBlockingQueue<KeyValuePair<?, ?>> outputQueue) throws IOException,
      InterruptedException {

    Class<?> keyOutClass = rConf.getClass(REDUCER_OUTPUT_KEY_CLASS,
        Object.class);
    Class<?> valueOutClass = rConf.getClass(REDUCER_OUTPUT_VALUE_CLASS,
        Object.class);
    RecordWriter rw = new ChainRecordWriter(keyOutClass, valueOutClass,
        outputQueue, rConf);
    Reducer.Context reducerContext = createReduceContext(rw,
        (ReduceContext) inputContext, rConf);
    ReduceRunner runner = new ReduceRunner(reducerContext, reducer, rw);
    threads.add(runner);
  }

  // start all the threads 线程集合一个接一个的去执行
  void startAllThreads() {
    for (Thread thread : threads) {
      thread.start();
    }
  }
  
  // wait till all threads finish
  void joinAllThreads() throws IOException, InterruptedException {
    for (Thread thread : threads) {
      thread.join();
    }
    Throwable th = getThrowable();
    if (th != null) {
      if (th instanceof IOException) {
        throw (IOException) th;
      } else if (th instanceof InterruptedException) {
        throw (InterruptedException) th;
      } else {
        throw new RuntimeException(th);
      }
    }
  }

  // interrupt all threads
  private synchronized void interruptAllThreads() {
    for (Thread th : threads) {
      th.interrupt();
    }
    for (ChainBlockingQueue<?> queue : blockingQueues) {
      queue.interrupt();
    }
  }

  /**
   * Returns the prefix to use for the configuration of the chain depending if
   * it is for a Mapper or a Reducer.
   * 
   * @param isMap
   *          TRUE for Mapper, FALSE for Reducer.
   * @return the prefix to use.
   * 获取前缀
   */
  protected static String getPrefix(boolean isMap) {
    return (isMap) ? CHAIN_MAPPER : CHAIN_REDUCER;
  }

  /**
   * 获取该prefix在conf中应该要被设置的最大索引号
   */
  protected static int getIndex(Configuration conf, String prefix) {
    return conf.getInt(prefix + CHAIN_MAPPER_SIZE, 0);
  }

  /**
   * Creates a {@link Configuration} for the Map or Reduce in the chain.
   * 
   * <p>
   * It creates a new Configuration using the chain job's Configuration as base
   * and adds to it the configuration properties for the chain element. The keys
   * of the chain element Configuration have precedence over the given
   * Configuration.
   * </p>
   * 
   * @param jobConf
   *          the chain job's Configuration.
   * @param confKey
   *          the key for chain element configuration serialized in the chain
   *          job's Configuration.
   * @return a new Configuration aggregating the chain job's Configuration with
   *         the chain element configuration properties.
   * 从主配置文件中还原任务的私有配置文件对象
   * 参数confKey是用前缀+map+索引,或者前缀+reduce组成的key,用于在主配置文件中作为查询key使用
   */
  protected static Configuration getChainElementConf(Configuration jobConf,
      String confKey) {
    Configuration conf = null;
    try {
      Stringifier<Configuration> stringifier = 
        new DefaultStringifier<Configuration>(jobConf, Configuration.class);
      String confString = jobConf.get(confKey, null);
      if (confString != null) {
        conf = stringifier.fromString(jobConf.get(confKey, null));
      }
    } catch (IOException ioex) {
      throw new RuntimeException(ioex);
    }
    // we have to do this because the Writable desearialization clears all
    // values set in the conf making not possible do a
    // new Configuration(jobConf) in the creation of the conf above
    jobConf = new Configuration(jobConf);

    if (conf != null) {
      for (Map.Entry<String, String> entry : conf) {
        jobConf.set(entry.getKey(), entry.getValue());
      }
    }
    return jobConf;
  }

  /**
   * Adds a Mapper class to the chain job.
   * 添加一个mapp对象到chain链表中
   * <p/>
   * The configuration properties of the chain job have precedence over the
   * configuration properties of the Mapper.
   * 
   * @param isMap 是否是map任务
   *          indicates if the Chain is for a Mapper or for a Reducer.
   * @param job 整个job总的配置文件
   *          chain job.
   * @param klass mapper的class
   *          the Mapper class to add.
   * @param inputKeyClass 输入对象
   *          mapper input key class.
   * @param inputValueClass 输入value对象
   *          mapper input value class.
   * @param outputKeyClass 输出key对象
   *          mapper output key class.
   * @param outputValueClass 输出value对象
   *          mapper output value class.
   * @param mapperConf 属于该job的配置文件
   *          a configuration for the Mapper class. It is recommended to use a
   *          Configuration without default values using the
   *          <code>Configuration(boolean loadDefaults)</code> constructor with
   *          FALSE.
   * 添加一个mapp对象 ,有可能是reduce操作之后又添加的map对象,因此是需要校验checkReducerAlreadySet的
   * 1.校验
   * 2.获取该map的索引顺序
   * 3.在主job配置文件中设置key为前缀_map_索引 value是map对应的实现类
   * 4.校验输入是否是上一个的输出
   * 5.设置该map私有的配置文件,将该map的输入输出key-value设置到私有的配置中
   * 6.强私有的map配置添加到主配置文件中
   * 7.累加Map的索引数量+1
   */
  @SuppressWarnings("unchecked")
  protected static void addMapper(boolean isMap, Job job,
      Class<? extends Mapper> klass, Class<?> inputKeyClass,
      Class<?> inputValueClass, Class<?> outputKeyClass,
      Class<?> outputValueClass, Configuration mapperConf) {
    String prefix = getPrefix(isMap);//获取前缀字符串
    Configuration jobConf = job.getConfiguration();//获取总的配置文件信息

    // if a reducer chain check the Reducer has been already set
    //如果是reduce对象,则要检查reduce是否已经被设置了,因为reduce只能被设置一次,因此出现这个检查
    checkReducerAlreadySet(isMap, jobConf, prefix, true);

    // set the mapper class
    int index = getIndex(jobConf, prefix);
    //设置序列号 与对应的calss对象
    jobConf.setClass(prefix + CHAIN_MAPPER_CLASS + index, klass, Mapper.class);

    /**
     * 主要检查热媒的输入key与value是否是上一个结果的输出key-value
     * 1.检查这个在链条中的map任务的key与value的输出是否匹配前一个map的输出key与value
     * 2.如果是reduce连,并且第一个mapp被添加,要检查key和value的参数是否是reduce的输出key与value
     */
    validateKeyValueTypes(isMap, jobConf, inputKeyClass, inputValueClass,
        outputKeyClass, outputValueClass, index, prefix);

    /**
     * 1.向该task的私有配置文件中设置输出/输出的key-value
     * 2.将该task的配置文件写入总job的配置文件中
     * 3.将index索引累加1
     */
    setMapperConf(isMap, jobConf, inputKeyClass, inputValueClass,
        outputKeyClass, outputValueClass, mapperConf, index, prefix);
  }

  // if a reducer chain check the Reducer has been already set or not
  /**
   * 如果是reduce对象,则要检查reduce是否已经被设置了,因为reduce只能被设置一次,因此出现这个检查
   * @param isMap 是否是map任务
   * @param jobConf job的配置文件对象
   * @param prefix 前缀字符串
   * @param shouldSet true表示该reduce已经被设置了,false表示没有被设置
   */
  protected static void checkReducerAlreadySet(boolean isMap,
      Configuration jobConf, String prefix, boolean shouldSet) {
    if (!isMap) {//reduce任务才被执行
      if (shouldSet) {//说明认为已经reduce被设置了
        if (jobConf.getClass(prefix + CHAIN_REDUCER_CLASS, null) == null) {//如果还没有查询到reduce,说明有异常,因为他认为已经被设置了reduce
          throw new IllegalStateException(
              "A Mapper can be added to the chain only after the Reducer has "
                  + "been set");
        }
      } else {//说明认为reduce没有被设置
        if (jobConf.getClass(prefix + CHAIN_REDUCER_CLASS, null) != null) {//发现有内容被查询到,说明已经被设置了,因此会有异常
          throw new IllegalStateException("Reducer has been already set");
        }
      }
    }
  }

  /**
   * 
   * @param isMap 是否是map任务
   * @param jobConf job的总的配置文件
   * @param inputKeyClass 输入
   * @param inputValueClass 输入
   * @param outputKeyClass 输出
   * @param outputValueClass 输出
   * @param index 该map/redece的索引下标
   * @param prefix 该map/redece的前缀
   * 
   * 主要检查热媒的输入key与value是否是上一个结果的输出key-value
   * 1.检查这个在链条中的map任务的key与value的输出是否匹配前一个map的输出key与value
   * 2.如果是reduce连,并且第一个mapp被添加,要检查key和value的参数是否是reduce的输出key与value
   */
  protected static void validateKeyValueTypes(boolean isMap,
      Configuration jobConf, Class<?> inputKeyClass, Class<?> inputValueClass,
      Class<?> outputKeyClass, Class<?> outputValueClass, int index,
      String prefix) {
    // if it is a reducer chain and the first Mapper is being added check the
    // key and value input classes of the mapper match those of the reducer
    // output.如果是reduce连,并且第一个mapp被添加,要检查key和value的参数是否是reduce的输出key与value
    if (!isMap && index == 0) {//如果是reduce,并且index=0,即reduce - map
      Configuration reducerConf = getChainElementConf(jobConf, prefix
          + CHAIN_REDUCER_CONFIG);
      //一定保证reduce的输出key,就是这个Map的输入key
      if (!inputKeyClass.isAssignableFrom(reducerConf.getClass(
          REDUCER_OUTPUT_KEY_CLASS, null))) {
        throw new IllegalArgumentException("The Reducer output key class does"
            + " not match the Mapper input key class");
      }
      //一定保证reduce的输出value,就是这个Map的输入value
      if (!inputValueClass.isAssignableFrom(reducerConf.getClass(
          REDUCER_OUTPUT_VALUE_CLASS, null))) {
        throw new IllegalArgumentException("The Reducer output value class"
            + " does not match the Mapper input value class");
      }
    } else if (index > 0) {//如果是map任务,并且index>0
      // check the that the new Mapper in the chain key and value input classes
      // match those of the previous Mapper output.检查这个在链条中的map任务的key与value的输出是否匹配前一个map的输出key与value
      Configuration previousMapperConf = getChainElementConf(jobConf, prefix
          + CHAIN_MAPPER_CONFIG + (index - 1));
      if (!inputKeyClass.isAssignableFrom(previousMapperConf.getClass(
          MAPPER_OUTPUT_KEY_CLASS, null))) {
        throw new IllegalArgumentException("The specified Mapper input key class does"
            + " not match the previous Mapper's output key class.");
      }
      if (!inputValueClass.isAssignableFrom(previousMapperConf.getClass(
          MAPPER_OUTPUT_VALUE_CLASS, null))) {
        throw new IllegalArgumentException("The specified Mapper input value class"
            + " does not match the previous Mapper's output value class.");
      }
    }
  }

  /**
   * 
   * @param isMap 是否是map任务
   * @param jobConf job的总配置文件
   * @param inputKeyClass
   * @param inputValueClass
   * @param outputKeyClass
   * @param outputValueClass
   * @param mapperConf 该task的配置文件
   * @param index 索引属于第几个
   * @param prefix 前缀字符串 
   * 
   * 1.向该task的私有配置文件中设置输出/输出的key-value
   * 2.将该task的配置文件写入总job的配置文件中
   * 3.将index索引累加1
   */
  protected static void setMapperConf(boolean isMap, Configuration jobConf,
      Class<?> inputKeyClass, Class<?> inputValueClass,
      Class<?> outputKeyClass, Class<?> outputValueClass,
      Configuration mapperConf, int index, String prefix) {
    // if the Mapper does not have a configuration, create an empty one
    if (mapperConf == null) {
      // using a Configuration without defaults to make it lightweight.
      // still the chain's conf may have all defaults and this conf is
      // overlapped to the chain configuration one.
      mapperConf = new Configuration(true);
    }

    //向该task任务的私有配置文件中配置输入/输出的key-value
    // store the input/output classes of the mapper in the mapper conf
    mapperConf.setClass(MAPPER_INPUT_KEY_CLASS, inputKeyClass, Object.class);
    mapperConf
        .setClass(MAPPER_INPUT_VALUE_CLASS, inputValueClass, Object.class);
    mapperConf.setClass(MAPPER_OUTPUT_KEY_CLASS, outputKeyClass, Object.class);
    mapperConf.setClass(MAPPER_OUTPUT_VALUE_CLASS, outputValueClass,
        Object.class);
    // serialize the mapper configuration in the chain configuration.向总的配置文件中设置该task对应的配置信息
    Stringifier<Configuration> stringifier = 
      new DefaultStringifier<Configuration>(jobConf, Configuration.class);
    try {
      jobConf.set(prefix + CHAIN_MAPPER_CONFIG + index, stringifier
          .toString(new Configuration(mapperConf)));
    } catch (IOException ioEx) {
      throw new RuntimeException(ioEx);
    }

    // increment the chain counter 累加index序号
    jobConf.setInt(prefix + CHAIN_MAPPER_SIZE, index + 1);
  }

  /**
   * Sets the Reducer class to the chain job.
   * 
   * <p/>
   * The configuration properties of the chain job have precedence over the
   * configuration properties of the Reducer.
   * 
   * @param job
   *          the chain job.
   * @param klass
   *          the Reducer class to add.
   * @param inputKeyClass
   *          reducer input key class.
   * @param inputValueClass
   *          reducer input value class.
   * @param outputKeyClass
   *          reducer output key class.
   * @param outputValueClass
   *          reducer output value class.
   * @param reducerConf
   *          a configuration for the Reducer class. It is recommended to use a
   *          Configuration without default values using the
   *          <code>Configuration(boolean loadDefaults)</code> constructor with
   *          FALSE.
   *          设置reduce
   *1.校验
   *2.在主job配置中设置reduce的class
   *3.在reduce自己的配置文件中配置输入输出的key-value对象
   *4.将reduce自己的配置文件写入主job的配置文件中        
   */
  @SuppressWarnings("unchecked")
  protected static void setReducer(Job job, Class<? extends Reducer> klass,
      Class<?> inputKeyClass, Class<?> inputValueClass,
      Class<?> outputKeyClass, Class<?> outputValueClass,
      Configuration reducerConf) {
    String prefix = getPrefix(false);
    Configuration jobConf = job.getConfiguration();
    checkReducerAlreadySet(false, jobConf, prefix, false);

    jobConf.setClass(prefix + CHAIN_REDUCER_CLASS, klass, Reducer.class);

    setReducerConf(jobConf, inputKeyClass, inputValueClass, outputKeyClass,
        outputValueClass, reducerConf, prefix);
  }

  /**
   * 设置该task任务的私有conf
   * 并且将其设置到job的全局conf中
   */
  protected static void setReducerConf(Configuration jobConf,
      Class<?> inputKeyClass, Class<?> inputValueClass,
      Class<?> outputKeyClass, Class<?> outputValueClass,
      Configuration reducerConf, String prefix) {
    // if the Reducer does not have a Configuration, create an empty one
    if (reducerConf == null) {
      // using a Configuration without defaults to make it lightweight.
      // still the chain's conf may have all defaults and this conf is
      // overlapped to the chain's Configuration one.
      reducerConf = new Configuration(false);
    }

    // store the input/output classes of the reducer in
    // the reducer configuration
    reducerConf.setClass(REDUCER_INPUT_KEY_CLASS, inputKeyClass, Object.class);
    reducerConf.setClass(REDUCER_INPUT_VALUE_CLASS, inputValueClass,
        Object.class);
    reducerConf
        .setClass(REDUCER_OUTPUT_KEY_CLASS, outputKeyClass, Object.class);
    reducerConf.setClass(REDUCER_OUTPUT_VALUE_CLASS, outputValueClass,
        Object.class);

    // serialize the reducer configuration in the chain's configuration.
    Stringifier<Configuration> stringifier = 
      new DefaultStringifier<Configuration>(jobConf, Configuration.class);
    try {
      jobConf.set(prefix + CHAIN_REDUCER_CONFIG, stringifier
          .toString(new Configuration(reducerConf)));
    } catch (IOException ioEx) {
      throw new RuntimeException(ioEx);
    }
  }

  /**
   * Setup the chain.
   * 
   * @param jobConf
   *          chain job's {@link Configuration}.
   * 实例化该链条中所有的map-reduce对象,并且解析每一个任务的配置文件,存储配置文件在内存中         
   */
  @SuppressWarnings("unchecked")
  void setup(Configuration jobConf) {
    String prefix = getPrefix(isMap);

    int index = jobConf.getInt(prefix + CHAIN_MAPPER_SIZE, 0);
    for (int i = 0; i < index; i++) {
      Class<? extends Mapper> klass = jobConf.getClass(prefix
          + CHAIN_MAPPER_CLASS + i, null, Mapper.class);//获取map对象
      Configuration mConf = getChainElementConf(jobConf, prefix
          + CHAIN_MAPPER_CONFIG + i);//还原该map对应的conf私有配置文件
      confList.add(mConf);
      Mapper mapper = ReflectionUtils.newInstance(klass, mConf);//实例化Map对象
      mappers.add(mapper);
    }

    //reduce的对象
    Class<? extends Reducer> klass = jobConf.getClass(prefix
        + CHAIN_REDUCER_CLASS, null, Reducer.class);
    if (klass != null) {
      rConf = getChainElementConf(jobConf, prefix + CHAIN_REDUCER_CONFIG);//reduce对应的conf私有配置文件
      reducer = ReflectionUtils.newInstance(klass, rConf);//实例化reduce对象
    }
  }

  @SuppressWarnings("unchecked")
  List<Mapper> getAllMappers() {
    return mappers;
  }

  /**
   * Returns the Reducer instance in the chain.
   * 
   * @return the Reducer instance in the chain or NULL if none.
   */
  Reducer<?, ?, ?, ?> getReducer() {
    return reducer;
  }
  
  /**
   * Creates a ChainBlockingQueue with KeyValuePair as element
   * 
   * @return the ChainBlockingQueue
   */
  ChainBlockingQueue<KeyValuePair<?, ?>> createBlockingQueue() {
    return new ChainBlockingQueue<KeyValuePair<?, ?>>();
  }

  /**
   * A blocking queue with one element.
   *   
   * @param <E>
   */
  class ChainBlockingQueue<E> {
    E element = null;
    boolean isInterrupted = false;
    
    ChainBlockingQueue() {
      blockingQueues.add(this);
    }

    synchronized void enqueue(E e) throws InterruptedException {
      while (element != null) {
        if (isInterrupted) {
          throw new InterruptedException();
        }
        this.wait();
      }
      element = e;
      this.notify();
    }

    synchronized E dequeue() throws InterruptedException {
      while (element == null) {
        if (isInterrupted) {
          throw new InterruptedException();
        }
        this.wait();
      }
      E e = element;
      element = null;
      this.notify();
      return e;
    }

    synchronized void interrupt() {
      isInterrupted = true;
      this.notifyAll();
    }
  }
}
