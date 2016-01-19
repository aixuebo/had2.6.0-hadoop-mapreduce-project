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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.Chain.ChainBlockingQueue;

import java.io.IOException;

/**
 * The ChainReducer class allows to chain multiple Mapper classes after a
 * Reducer within the Reducer task.
 * 允许多个map跟随在reduce后面,即先执行reduce,再执行map集合
 * 
 * <p>
 * For each record output by the Reducer, the Mapper classes are invoked in a
 * chained (or piped) fashion. The output of the reducer becomes the input of
 * the first mapper and output of first becomes the input of the second, and so
 * on until the last Mapper, the output of the last Mapper will be written to
 * the task's output.
 * </p>
 * <p>
 * The key functionality of this feature is that the Mappers in the chain do not
 * need to be aware that they are executed after the Reducer or in a chain. This
 * enables having reusable specialized Mappers that can be combined to perform
 * composite operations within a single task.
 * </p>
 * <p>
 * Special care has to be taken when creating chains that the key/values output
 * by a Mapper are valid for the following Mapper in the chain. It is assumed
 * all Mappers and the Reduce in the chain use matching output and input key and
 * value classes as no conversion is done by the chaining code.
 * </p>
 * </p> Using the ChainMapper and the ChainReducer classes is possible to
 * compose Map/Reduce jobs that look like <code>[MAP+ / REDUCE MAP*]</code>. And
 * immediate benefit of this pattern is a dramatic reduction in disk IO. </p>
 * <p>
 * IMPORTANT: There is no need to specify the output key/value classes for the
 * ChainReducer, this is done by the setReducer or the addMapper for the last
 * element in the chain.
 * </p>
 * ChainReducer usage pattern:
 * <p/>
 * 
 * <pre>
 * ...
 * Job = new Job(conf);
 * ....
 * <p/>
 * Configuration reduceConf = new Configuration(false);
 * ...
 * ChainReducer.setReducer(job, XReduce.class, LongWritable.class, Text.class,
 *   Text.class, Text.class, true, reduceConf);
 * <p/>
 * ChainReducer.addMapper(job, CMap.class, Text.class, Text.class,
 *   LongWritable.class, Text.class, false, null);
 * <p/>
 * ChainReducer.addMapper(job, DMap.class, LongWritable.class, Text.class,
 *   LongWritable.class, LongWritable.class, true, null);
 * <p/>
 * ...
 * <p/>
 * job.waitForCompletion(true);
 * ...
 * </pre>
 * 
 * 该链条逻辑留存:
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

该链条是:
1.必须有reduce对象,
2.有map对象的话,reduce对象必须放第一个位置即可
3.如果没有map对象,则允许仅仅有reduce对象去执行
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ChainReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends
    Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  /**
   * Sets the {@link Reducer} class to the chain job.
   * 
   * <p>
   * The key and values are passed from one element of the chain to the next, by
   * value. For the added Reducer the configuration given for it,
   * <code>reducerConf</code>, have precedence over the job's Configuration.
   * This precedence is in effect when the task is running.
   * </p>
   * <p>
   * IMPORTANT: There is no need to specify the output key/value classes for the
   * ChainReducer, this is done by the setReducer or the addMapper for the last
   * element in the chain.
   * </p>
   * 
   * @param job
   *          the job
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
   */
  public static void setReducer(Job job, Class<? extends Reducer> klass,
      Class<?> inputKeyClass, Class<?> inputValueClass,
      Class<?> outputKeyClass, Class<?> outputValueClass,
      Configuration reducerConf) {
    job.setReducerClass(ChainReducer.class);
    job.setOutputKeyClass(outputKeyClass);
    job.setOutputValueClass(outputValueClass);
    Chain.setReducer(job, klass, inputKeyClass, inputValueClass,
        outputKeyClass, outputValueClass, reducerConf);
  }

  /**
   * Adds a {@link Mapper} class to the chain reducer.
   * 
   * <p>
   * The key and values are passed from one element of the chain to the next, by
   * value For the added Mapper the configuration given for it,
   * <code>mapperConf</code>, have precedence over the job's Configuration. This
   * precedence is in effect when the task is running.
   * </p>
   * <p>
   * IMPORTANT: There is no need to specify the output key/value classes for the
   * ChainMapper, this is done by the addMapper for the last mapper in the
   * chain.
   * </p>
   * 
   * @param job
   *          The job.
   * @param klass
   *          the Mapper class to add.
   * @param inputKeyClass
   *          mapper input key class.
   * @param inputValueClass
   *          mapper input value class.
   * @param outputKeyClass
   *          mapper output key class.
   * @param outputValueClass
   *          mapper output value class.
   * @param mapperConf
   *          a configuration for the Mapper class. It is recommended to use a
   *          Configuration without default values using the
   *          <code>Configuration(boolean loadDefaults)</code> constructor with
   *          FALSE.
   */
  public static void addMapper(Job job, Class<? extends Mapper> klass,
      Class<?> inputKeyClass, Class<?> inputValueClass,
      Class<?> outputKeyClass, Class<?> outputValueClass,
      Configuration mapperConf) throws IOException {
    job.setOutputKeyClass(outputKeyClass);
    job.setOutputValueClass(outputValueClass);
    Chain.addMapper(false, job, klass, inputKeyClass, inputValueClass,
        outputKeyClass, outputValueClass, mapperConf);
  }

  private Chain chain;

  protected void setup(Context context) {
    chain = new Chain(false);
    //真正的组装链条信息
    chain.setup(context.getConfiguration());
  }

  public void run(Context context) throws IOException, InterruptedException {
    setup(context);

    // if no reducer is set, just do nothing 必须有reduce对象
    if (chain.getReducer() == null) {
      return;
    }
    int numMappers = chain.getAllMappers().size();
    // if there are no mappers in chain, run the reducer
    if (numMappers == 0) {//如果没有map,则直接执行reduce
      chain.runReducer(context);
      return;
    }

    // add reducer and all mappers with proper context
    ChainBlockingQueue<Chain.KeyValuePair<?, ?>> inputqueue;
    ChainBlockingQueue<Chain.KeyValuePair<?, ?>> outputqueue;
    // add reducer
    outputqueue = chain.createBlockingQueue();//创建输出队列
    chain.addReducer(context, outputqueue);//添加reduce对象,从输出流输入,输出到队列中

    // add all mappers except last one 从队列中获取数据,输出到队列中
    for (int i = 0; i < numMappers - 1; i++) {//从第0个map,运行到最后一个map
      inputqueue = outputqueue;//reduce的输出,就是map的输入
      outputqueue = chain.createBlockingQueue();//创建新的输出
      chain.addMapper(inputqueue, outputqueue, context, i);//运行第i个Map对象
    }
    // add last mapper运行最后一个map,因为数据源不一样，输入源是队列,输出是输出流
    chain.addMapper(outputqueue, context, numMappers - 1);

    // start all threads按顺序一次启动job集合
    chain.startAllThreads();
    
    // wait for all threads
    chain.joinAllThreads();
  }
}
