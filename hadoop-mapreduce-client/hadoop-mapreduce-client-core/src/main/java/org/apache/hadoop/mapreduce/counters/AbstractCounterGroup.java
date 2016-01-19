/*
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

package org.apache.hadoop.mapreduce.counters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.util.ResourceBundles;
import org.apache.hadoop.util.StringInterner;

import com.google.common.collect.Iterators;

/**
 * An abstract class to provide common implementation of the
 * generic counter group in both mapred and mapreduce package.
 *
 * @param <T> type of the counter for the group
 * 
 * 该对象表示一个组对象，该组包含若干个count对象,count和group的个数由Limits限制，而Limits是有构造函数传递过来的
 */
@InterfaceAudience.Private
public abstract class AbstractCounterGroup<T extends Counter>
    implements CounterGroupBase<T> {

  private final String name;//组名称
  private String displayName;//组显示名称
  //key是该group包含的count集合,key是count的name,value是count对象
  private final ConcurrentMap<String, T> counters = new ConcurrentSkipListMap<String, T>();
      
  //该对象是AbstractCounters产生的
  private final Limits limits;//该组的限制,即包含组和count的name限制,以及count和group的个数限制

  public AbstractCounterGroup(String name, String displayName,
                              Limits limits) {
    this.name = name;
    this.displayName = displayName;
    this.limits = limits;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public synchronized String getDisplayName() {
    return displayName;
  }

  @Override
  public synchronized void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  //添加一个count,并且增加limits限制
  @Override
  public synchronized void addCounter(T counter) {
    counters.put(counter.getName(), counter);
    limits.incrCounters();
  }

  @Override
  public synchronized T addCounter(String counterName, String displayName,long value) {
    String saveName = Limits.filterCounterName(counterName);
    T counter = findCounterImpl(saveName, false);
    if (counter == null) {
      return addCounterImpl(saveName, displayName, value);
    }
    counter.setValue(value);
    return counter;
  }

  private T addCounterImpl(String name, String displayName, long value) {
    T counter = newCounter(name, displayName, value);
    addCounter(counter);
    return counter;
  }

  @Override
  public synchronized T findCounter(String counterName, String displayName) {
    // Take lock to avoid two threads not finding a counter and trying to add
    // the same counter.
    String saveName = Limits.filterCounterName(counterName);
    T counter = findCounterImpl(saveName, false);
    if (counter == null) {
      return addCounterImpl(saveName, displayName, 0);
    }
    return counter;
  }

  /**
   * 查找count,查找不到则创建(根据第二个参数)
   */
  @Override
  public T findCounter(String counterName, boolean create) {
    return findCounterImpl(Limits.filterCounterName(counterName), create);
  }

  // Lock the object. Cannot simply use concurrent constructs on the counters
  // data-structure (like putIfAbsent) because of localization, limits etc.
  //在该group下查找名称为参数counterName的count对象,如果查找不到,并且参数create=false,则返回null,否则创建一个count对象
  private synchronized T findCounterImpl(String counterName, boolean create) {
    T counter = counters.get(counterName);
    if (counter == null && create) {
      String localized =
          ResourceBundles.getCounterName(getName(), counterName, counterName);
      return addCounterImpl(counterName, localized, 0);
    }
    return counter;
  }

  @Override
  public T findCounter(String counterName) {
    return findCounter(counterName, true);
  }

  /**
   * Abstract factory method to create a new counter of type T
   * @param counterName of the counter
   * @param displayName of the counter
   * @param value of the counter
   * @return a new counter
   * 创建一个count对象
   */
  protected abstract T newCounter(String counterName, String displayName,
                                  long value);

  /**
   * Abstract factory method to create a new counter of type T
   * @return a new counter object
   */
  protected abstract T newCounter();

  //遍历该group的所有count对象
  @Override
  public Iterator<T> iterator() {
    return counters.values().iterator();
  }

  /**
   * GenericGroup ::= displayName #counter counter*
   */
  @Override
  public synchronized void write(DataOutput out) throws IOException {
    Text.writeString(out, displayName);
    WritableUtils.writeVInt(out, counters.size());
    for(Counter counter: counters.values()) {
      counter.write(out);
    }
  }

  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    displayName = StringInterner.weakIntern(Text.readString(in));
    counters.clear();
    int size = WritableUtils.readVInt(in);
    for (int i = 0; i < size; i++) {
      T counter = newCounter();
      counter.readFields(in);
      counters.put(counter.getName(), counter);
      limits.incrCounters();
    }
  }

  //获取该group下的count数量
  @Override
  public synchronized int size() {
    return counters.size();
  }

  @Override
  public synchronized boolean equals(Object genericRight) {
    if (genericRight instanceof CounterGroupBase<?>) {
      @SuppressWarnings("unchecked")
      CounterGroupBase<T> right = (CounterGroupBase<T>) genericRight;
      return Iterators.elementsEqual(iterator(), right.iterator());
    }
    return false;
  }

  @Override
  public synchronized int hashCode() {
    return counters.hashCode();
  }

  /**
   * 循环参数group下所有的count对象
   * 1.如果循环的count的name可以查询到,则累加参数组下的值。
   * 2.如果找不到该name的count,则创建一个count,并且累加值
   */
  @Override
  public void incrAllCounters(CounterGroupBase<T> rightGroup) {
    try {
      for (Counter right : rightGroup) {
        Counter left = findCounter(right.getName(), right.getDisplayName());
        left.increment(right.getValue());
      }
    } catch (LimitExceededException e) {
      counters.clear();
      throw e;
    }
  }
}
