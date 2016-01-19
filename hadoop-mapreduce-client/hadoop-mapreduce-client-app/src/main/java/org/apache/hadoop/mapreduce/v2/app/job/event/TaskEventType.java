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

package org.apache.hadoop.mapreduce.v2.app.job.event;

/**
 * Event types handled by Task.
 * 记录一个任务的事件类型
 */
public enum TaskEventType {

  //Producer:Client, Job
  T_KILL,//该任务被kill

  //Producer:Job
  T_SCHEDULE,//该任务呗调用
  T_RECOVER,//该任务呗覆盖,重新执行

  //Producer:Speculator
  T_ADD_SPEC_ATTEMPT,//增加了一个尝试任务,因为原始尝试任务运行的太慢了

  //Producer:TaskAttempt
  T_ATTEMPT_LAUNCHED,//该任务的尝试任务被启动了
  T_ATTEMPT_COMMIT_PENDING,//该任务的尝试任务提交等待
  T_ATTEMPT_FAILED,//该任务的尝试任务运行失败
  T_ATTEMPT_SUCCEEDED,//该任务的尝试任务运行成功
  T_ATTEMPT_KILLED//该任务的尝试任务运行kill
}
