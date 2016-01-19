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

import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This abstract class implements some common functionalities of the
 * the generic mapper, reducer and combiner classes of Aggregate.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueAggregatorJobBase<K1 extends WritableComparable<?>,
                                             V1 extends Writable>
{
  public static final String DESCRIPTOR = "mapreduce.aggregate.descriptor";//在配置文件中,描述的key
  public static final String DESCRIPTOR_NUM = 
    "mapreduce.aggregate.descriptor.num";//在配置文件中,描述的key的数量
  public static final String USER_JAR = "mapreduce.aggregate.user.jar.file";//运行job的jar包名字
  
  protected static ArrayList<ValueAggregatorDescriptor> aggregatorDescriptorList = null;

  /**
   * 1.根据描述的数量,解析所有的描述对象,并且设置到集合中
   * 默认是设置ValueAggregatorBaseDescriptor对象实例
   * 2.
   */
  public static void setup(Configuration job) {
    initializeMySpec(job);
    logSpec();
  }

  /**
   * 根据协议解析ValueAggregatorDescriptor对应的实现类,并且实例化该对象
   * @param spec UserDefined,+ValueAggregatorDescriptor.getName()  含义是协议,协议对应的ValueAggregatorDescriptor的实现类class对象
   */
  protected static ValueAggregatorDescriptor getValueAggregatorDescriptor(
      String spec, Configuration conf) {
    if (spec == null)
      return null;
    String[] segments = spec.split(",", -1);
    String type = segments[0];
    if (type.compareToIgnoreCase("UserDefined") == 0) {
      String className = segments[1];
      return new UserDefinedValueAggregatorDescriptor(className, conf);
    }
    return null;
  }

  /**
   * 根据描述的数量,解析所有的描述对象,并且设置到集合中
   * 解析配置文件中的描述信息
   */
  protected static ArrayList<ValueAggregatorDescriptor> getAggregatorDescriptors(
      Configuration conf) {
    int num = conf.getInt(DESCRIPTOR_NUM, 0);//获取描述的数量
    ArrayList<ValueAggregatorDescriptor> retv = 
      new ArrayList<ValueAggregatorDescriptor>(num);
    for (int i = 0; i < num; i++) {
      String spec = conf.get(DESCRIPTOR + "." + i);
      ValueAggregatorDescriptor ad = getValueAggregatorDescriptor(spec, conf);
      if (ad != null) {
        retv.add(ad);
      }
    }
    return retv;
  }

  /**
   * 根据描述的数量,解析所有的描述对象,并且设置到集合中
   * 默认是设置ValueAggregatorBaseDescriptor对象实例
   */
  private static void initializeMySpec(Configuration conf) {
    aggregatorDescriptorList = getAggregatorDescriptors(conf);
    if (aggregatorDescriptorList.size() == 0) {
      aggregatorDescriptorList
          .add(new UserDefinedValueAggregatorDescriptor(
              ValueAggregatorBaseDescriptor.class.getCanonicalName(), conf));
    }
  }

  protected static void logSpec() {
  }
}
