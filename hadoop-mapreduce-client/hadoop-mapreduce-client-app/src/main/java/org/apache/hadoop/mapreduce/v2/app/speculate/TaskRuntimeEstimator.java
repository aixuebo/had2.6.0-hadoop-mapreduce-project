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

package org.apache.hadoop.mapreduce.v2.app.speculate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;

/**
 * 任务运行的统计量
 */
public interface TaskRuntimeEstimator {
  
  //登记每一个尝试任务Id的时间
  public void enrollAttempt(TaskAttemptStatus reportedStatus, long timestamp);

  //获取每一个尝试任务的时间
  public long attemptEnrolledTime(TaskAttemptId attemptID);

  //更新每一个尝试任务的时间
  public void updateAttempt(TaskAttemptStatus reportedStatus, long timestamp);

  /**
   * 初始化方法
   */
  public void contextualize(Configuration conf, AppContext context);

  /**
   *
   * Find a maximum reasonable execution wallclock time.  Includes the time
   * already elapsed.
   *
   * Find a maximum reasonable execution time.  Includes the time
   * already elapsed.  If the projected total execution time for this task
   * ever exceeds its reasonable execution time, we may speculate it.
   *
   * @param id the {@link TaskId} of the task we are asking about
   * @return the task's maximum reasonable runtime, or MAX_VALUE if
   *         we don't have enough information to rule out any runtime,
   *         however long.
   *
   */
  public long thresholdRuntime(TaskId id);

  /**
   *
   * Estimate a task attempt's total runtime.  Includes the time already
   * elapsed.
   *
   * @param id the {@link TaskAttemptId} of the attempt we are asking about
   * @return our best estimate of the attempt's runtime, or {@code -1} if
   *         we don't have enough information yet to produce an estimate.
   *
   */
  public long estimatedRuntime(TaskAttemptId id);

  /**
   *
   * Estimates how long a new attempt on this task will take if we start
   *  one now
   *
   * @param id the {@link TaskId} of the task we are asking about
   * @return our best estimate of a new attempt's runtime, or {@code -1} if
   *         we don't have enough information yet to produce an estimate.
   *
   */
  public long estimatedNewAttemptRuntime(TaskId id);

  /**
   *
   * Computes the width of the error band of our estimate of the task
   *  runtime as returned by {@link #estimatedRuntime(TaskAttemptId)}
   *
   * @param id the {@link TaskAttemptId} of the attempt we are asking about
   * @return our best estimate of the attempt's runtime, or {@code -1} if
   *         we don't have enough information yet to produce an estimate.
   *
   */
  public long runtimeEstimateVariance(TaskAttemptId id);
}
