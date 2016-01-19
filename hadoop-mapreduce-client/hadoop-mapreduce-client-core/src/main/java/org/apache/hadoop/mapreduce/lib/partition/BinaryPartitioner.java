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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * <p>Partition {@link BinaryComparable} keys using a configurable part of 
 * the bytes array returned by {@link BinaryComparable#getBytes()}.</p>
 * 
 * <p>The subarray to be used for the partitioning can be defined by means
 * of the following properties:
 * <ul>
 *   <li>
 *     <i>mapreduce.partition.binarypartitioner.left.offset</i>:
 *     left offset in array (0 by default)
 *   </li>
 *   <li>
 *     <i>mapreduce.partition.binarypartitioner.right.offset</i>: 
 *     right offset in array (-1 by default)
 *   </li>
 * </ul>
 * Like in Python, both negative and positive offsets are allowed, but
 * the meaning is slightly different. In case of an array of length 5,
 * for instance, the possible offsets are:
 * <pre><code>
 *  +---+---+---+---+---+
 *  | B | B | B | B | B |
 *  +---+---+---+---+---+
 *    0   1   2   3   4
 *   -5  -4  -3  -2  -1
 * </code></pre>
 * The first row of numbers gives the position of the offsets 0...5 in 
 * the array; the second row gives the corresponding negative offsets. 
 * Contrary to Python, the specified subarray has byte <code>i</code> 
 * and <code>j</code> as first and last element, repectively, when 
 * <code>i</code> and <code>j</code> are the left and right offset.
 * 
 * <p>For Hadoop programs written in Java, it is advisable to use one of 
 * the following static convenience methods for setting the offsets:
 * <ul>
 *   <li>{@link #setOffsets}</li>
 *   <li>{@link #setLeftOffset}</li>
 *   <li>{@link #setRightOffset}</li>
 * </ul></p>
 * BinaryPatitioner继承于Partitioner< BinaryComparable ,V>，是Partitioner的偏特化子类。该类提供leftOffset和rightOffset，在计算which reducer时仅对键值K的[rightOffset，leftOffset]这个区间取hash。
 * 正值,为0到n-1
 * 负值,-n的公式为 length-n,例如lengh = 5 ,n = -1,则-1的取5-1=4的坐标值，即arr[4]
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BinaryPartitioner<V> extends Partitioner<BinaryComparable, V> 
  implements Configurable {

  public static final String LEFT_OFFSET_PROPERTY_NAME = 
    "mapreduce.partition.binarypartitioner.left.offset";
  public static final String RIGHT_OFFSET_PROPERTY_NAME = 
    "mapreduce.partition.binarypartitioner.right.offset";
  
  /**
   * Set the subarray to be used for partitioning to 
   * <code>bytes[left:(right+1)]</code> in Python syntax.
   * 
   * @param conf configuration object
   * @param left left Python-style offset
   * @param right right Python-style offset
   */
  public static void setOffsets(Configuration conf, int left, int right) {
    conf.setInt(LEFT_OFFSET_PROPERTY_NAME, left);
    conf.setInt(RIGHT_OFFSET_PROPERTY_NAME, right);
  }
  
  /**
   * Set the subarray to be used for partitioning to 
   * <code>bytes[offset:]</code> in Python syntax.
   * 
   * @param conf configuration object
   * @param offset left Python-style offset
   */
  public static void setLeftOffset(Configuration conf, int offset) {
    conf.setInt(LEFT_OFFSET_PROPERTY_NAME, offset);
  }
  
  /**
   * Set the subarray to be used for partitioning to 
   * <code>bytes[:(offset+1)]</code> in Python syntax.
   * 
   * @param conf configuration object
   * @param offset right Python-style offset
   */
  public static void setRightOffset(Configuration conf, int offset) {
    conf.setInt(RIGHT_OFFSET_PROPERTY_NAME, offset);
  }
  
  
  private Configuration conf;
  private int leftOffset, rightOffset;//从左边数第几个,下标从0开始计算
  
  public void setConf(Configuration conf) {
    this.conf = conf;
    leftOffset = conf.getInt(LEFT_OFFSET_PROPERTY_NAME, 0);
    rightOffset = conf.getInt(RIGHT_OFFSET_PROPERTY_NAME, -1);
  }
  
  public Configuration getConf() {
    return conf;
  }
  
  /** 
   * Use (the specified slice of the array returned by) 
   * {@link BinaryComparable#getBytes()} to partition. 
   * 取key的[leftOffset,rightOffset]之间进行hash
   */
  @Override
  public int getPartition(BinaryComparable key, V value, int numPartitions) {
    int length = key.getLength();
    int leftIndex = (leftOffset + length) % length;
    int rightIndex = (rightOffset + length) % length;
    System.out.println(leftIndex+"==="+rightIndex);
    int hash = WritableComparator.hashBytes(key.getBytes(), 
      leftIndex, rightIndex - leftIndex + 1);
    return (hash & Integer.MAX_VALUE) % numPartitions;
  }
  
}
