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

package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A context object that allows input and output from the task. It is only
 * supplied to the {@link Mapper} or {@link Reducer}.
 * @param <KEYIN> the input key type for the task
 * @param <VALUEIN> the input value type for the task
 * @param <KEYOUT> the output key type for the task
 * @param <VALUEOUT> the output value type for the task
 *  * 尝试任务的上下文对象
 * 继承JobContext,说明从任务的上下文中可以获取作业job的一些信息
 * 
 * 对尝试任务的上下文对象进行了扩展,该扩展说明该对象可以获取下一个key-vakue时候存在等信息
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TaskInputOutputContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
       extends TaskAttemptContext {

  /**
   * Advance to the next key, value pair, returning null if at end.
   * @return the key object that was read into, or null if no more
   * 下一个key-vakue时候存在
   */
  public boolean nextKeyValue() throws IOException, InterruptedException;
 
  /**
   * Get the current key.
   * @return the current key object or null if there isn't one
   * @throws IOException
   * @throws InterruptedException
   * 获取当前key
   */
  public KEYIN getCurrentKey() throws IOException, InterruptedException;

  /**
   * Get the current value.
   * @return the value object that was read into
   * @throws IOException
   * @throws InterruptedException
   * 获取当前value
   */
  public VALUEIN getCurrentValue() throws IOException, InterruptedException;

  /**
   * Generate an output key/value pair.
   * 将信息写入到输出流中
   */
  public void write(KEYOUT key, VALUEOUT value) 
      throws IOException, InterruptedException;

  /**
   * Get the {@link OutputCommitter} for the task-attempt.
   * @return the <code>OutputCommitter</code> for the task-attempt
   */
  public OutputCommitter getOutputCommitter();
}
