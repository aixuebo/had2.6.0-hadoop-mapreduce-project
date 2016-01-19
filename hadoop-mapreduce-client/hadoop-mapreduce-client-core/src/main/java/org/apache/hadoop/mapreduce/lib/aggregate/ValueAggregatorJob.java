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

package org.apache.hadoop.mapreduce.lib.aggregate;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This is the main class for creating a map/reduce job using Aggregate
 * framework. The Aggregate is a specialization of map/reduce framework,
 * specializing for performing various simple aggregations.
 * 创建一个mr任务的聚类框架,聚类框架是一个专门的框架,专门执行各种各样简单的聚类统计
 * 
 * Generally speaking, in order to implement an application using Map/Reduce
 * model, the developer is to implement Map and Reduce functions (and possibly
 * combine function). However, a lot of applications related to counting and
 * statistics computing have very similar characteristics. Aggregate abstracts
 * out the general patterns of these functions and implementing those patterns.
 * In particular, the package provides generic mapper/redducer/combiner 
 * classes, and a set of built-in value aggregators, and a generic utility 
 * class that helps user create map/reduce jobs using the generic class. 
 * The built-in aggregators include:
 * 一般来说,一个应用使用mr模型,开发者需要实现map-reduce功能,有时可能要实现combine功能,
 * 然而,许多应用关联统计计算是非常类似的特性,聚合抽象输出一些模式功能,并且实现这些功能。
 * 
 * 
 * sum over numeric values count the number of distinct values compute the
 * histogram of values compute the minimum, maximum, media,average, standard
 * deviation of numeric values
 * 
 * The developer using Aggregate will need only to provide a plugin class
 * conforming to the following interface:
 * 
 * public interface ValueAggregatorDescriptor { public ArrayList<Entry>
 * generateKeyValPairs(Object key, Object value); public void
 * configure(Configuration conf); }
 * 
 * The package also provides a base class, ValueAggregatorBaseDescriptor,
 * implementing the above interface. The user can extend the base class and
 * implement generateKeyValPairs accordingly.
 * 
 * The primary work of generateKeyValPairs is to emit one or more key/value
 * pairs based on the input key/value pair. The key in an output key/value pair
 * encode two pieces of information: aggregation type and aggregation id. The
 * value will be aggregated onto the aggregation id according the aggregation
 * type.
 * 
 * This class offers a function to generate a map/reduce job using Aggregate
 * framework. The function takes the following parameters: input directory spec
 * input format (text or sequence file) output directory a file specifying the
 * user plugin class
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueAggregatorJob {

  /**
   * 创建JobControl任务,包含一个或者一个以上job
   */
  public static JobControl createValueAggregatorJobs(String args[],
    Class<? extends ValueAggregatorDescriptor>[] descriptors) 
  throws IOException {
    
    JobControl theControl = new JobControl("ValueAggregatorJobs");
    ArrayList<ControlledJob> dependingJobs = new ArrayList<ControlledJob>();
    Configuration conf = new Configuration();
    if (descriptors != null) {
      conf = setAggregatorDescriptors(descriptors);
    }
    Job job = createValueAggregatorJob(conf, args);
    //设置一个任务
    ControlledJob cjob = new ControlledJob(job, dependingJobs);
    theControl.addJob(cjob);
    return theControl;
  }

  /**
   * 创建JobControl任务,包含一个或者一个以上job
   */
  public static JobControl createValueAggregatorJobs(String args[]) 
      throws IOException {
    return createValueAggregatorJobs(args, null);
  }
  
  /**
   * Create an Aggregate based map/reduce job.
   * 
   * @param conf The configuration for job
   * @param args the arguments used for job creation. Generic hadoop
   * arguments are accepted.
   * @return a Job object ready for submission.
   * 
   * @throws IOException
   * @see GenericOptionsParser
   * 创建一个job
   * 
   * 参数包含
   * 1.输入路径,可以输入多个路径,用分隔符分隔即可
   * 2.输出路径
   * 3.多少个reduce
   * 4.textinputformat(表示输入是TextInputFormat类型),否则是SequenceFileInputFormat类型
   * 5.配置文件路径,用户加载一些配置信息文件,只能是一个配置文件
   * 6.jobName设置名字
   */
  public static Job createValueAggregatorJob(Configuration conf, String args[])
      throws IOException {

    GenericOptionsParser genericParser 
      = new GenericOptionsParser(conf, args);
    args = genericParser.getRemainingArgs();
    
    if (args.length < 2) {
      System.out.println("usage: inputDirs outDir "
          + "[numOfReducer [textinputformat|seq [specfile [jobName]]]]");
      GenericOptionsParser.printGenericCommandUsage(System.out);
      System.exit(2);
    }
    String inputDir = args[0];
    String outputDir = args[1];
    int numOfReducers = 1;
    if (args.length > 2) {
      numOfReducers = Integer.parseInt(args[2]);
    }

    Class<? extends InputFormat> theInputFormat = null;
    if (args.length > 3 && 
        args[3].compareToIgnoreCase("textinputformat") == 0) {
      theInputFormat = TextInputFormat.class;
    } else {
      theInputFormat = SequenceFileInputFormat.class;
    }

    Path specFile = null;

    if (args.length > 4) {
      specFile = new Path(args[4]);
    }

    String jobName = "";
    
    if (args.length > 5) {
      jobName = args[5];
    }

    if (specFile != null) {
      conf.addResource(specFile);
    }
    
    //在配置文件中加载配置的运行job的jar包名字
    String userJarFile = conf.get(ValueAggregatorJobBase.USER_JAR);
    if (userJarFile != null) {
      conf.set(MRJobConfig.JAR, userJarFile);
    }

    Job theJob = new Job(conf);
    if (userJarFile == null) {
      theJob.setJarByClass(ValueAggregator.class);
    }
    
    theJob.setJobName("ValueAggregatorJob: " + jobName);

    FileInputFormat.addInputPaths(theJob, inputDir);

    theJob.setInputFormatClass(theInputFormat);
    
    theJob.setMapperClass(ValueAggregatorMapper.class);
    FileOutputFormat.setOutputPath(theJob, new Path(outputDir));
    theJob.setOutputFormatClass(TextOutputFormat.class);
    theJob.setMapOutputKeyClass(Text.class);
    theJob.setMapOutputValueClass(Text.class);
    theJob.setOutputKeyClass(Text.class);
    theJob.setOutputValueClass(Text.class);
    theJob.setReducerClass(ValueAggregatorReducer.class);
    theJob.setCombinerClass(ValueAggregatorCombiner.class);
    theJob.setNumReduceTasks(numOfReducers);
    return theJob;
  }

  public static Job createValueAggregatorJob(String args[], 
      Class<? extends ValueAggregatorDescriptor>[] descriptors) 
      throws IOException {
    return createValueAggregatorJob(
             setAggregatorDescriptors(descriptors), args);
  }
  
  public static Configuration setAggregatorDescriptors(
      Class<? extends ValueAggregatorDescriptor>[] descriptors) {
    Configuration conf = new Configuration();
    conf.setInt(ValueAggregatorJobBase.DESCRIPTOR_NUM, descriptors.length);
    //specify the aggregator descriptors
    for(int i=0; i< descriptors.length; i++) {
      //设置key:mapreduce.aggregate.descriptor+i value:UserDefined,+ValueAggregatorDescriptor.getName()
      //value的含义是协议,协议对应的ValueAggregatorDescriptor的实现类class对象
      conf.set(ValueAggregatorJobBase.DESCRIPTOR + i, 
               "UserDefined," + descriptors[i].getName());
    }
    return conf;
  }
  
  /**
   * create and run an Aggregate based map/reduce job.
   * 
   * @param args the arguments used for job creation
   * @throws IOException
   */
  public static void main(String args[]) 
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = ValueAggregatorJob.createValueAggregatorJob(
                new Configuration(), args);
    int ret = job.waitForCompletion(true) ? 0 : 1;
    System.exit(ret);
  }
}
