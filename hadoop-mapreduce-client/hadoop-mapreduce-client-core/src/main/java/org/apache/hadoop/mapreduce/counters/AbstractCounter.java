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

import com.google.common.base.Objects;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapreduce.Counter;

/**
 * An abstract counter class to provide common implementation of
 * the counter interface in both mapred and mapreduce packages.
 * count对象的抽象信息,对equals和hashCode进行抽象复写
 */
@InterfaceAudience.Private
public abstract class AbstractCounter implements Counter {

  @Override @Deprecated
  public void setDisplayName(String name) {}

  @Override
  public synchronized boolean equals(Object genericRight) {
    if (genericRight instanceof Counter) {
      synchronized (genericRight) {
        Counter right = (Counter) genericRight;
        return getName().equals(right.getName()) &&
               getDisplayName().equals(right.getDisplayName()) &&
               getValue() == right.getValue();
      }
    }
    return false;
  }

  @Override
  public synchronized int hashCode() {
    return Objects.hashCode(getName(), getDisplayName(), getValue());
  }
}