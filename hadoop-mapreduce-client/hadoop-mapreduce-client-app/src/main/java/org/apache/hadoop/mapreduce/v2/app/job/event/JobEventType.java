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
 * Event types handled by Job.
 * job的事件类型
 */
public enum JobEventType {

  //Producer:Client
  JOB_KILL,

  //Producer:MRAppMaster
  JOB_INIT,//初始化
  JOB_INIT_FAILED,//初始化失败
  JOB_START,//开始启动job

  //Producer:Task
  JOB_TASK_COMPLETED,//一个task完成,job的一个task完成了,因此该类包含taskId和task完成时候的状态
  JOB_MAP_TASK_RESCHEDULED,
  JOB_TASK_ATTEMPT_COMPLETED,//一个尝试任务完成

  //Producer:CommitterEventHandler
  JOB_SETUP_COMPLETED,//job的setup任务完成
  JOB_SETUP_FAILED,//job的setup任务失败
  JOB_COMMIT_COMPLETED,//job的commit任务完成
  JOB_COMMIT_FAILED,//job的commit任务失败
  JOB_ABORT_COMPLETED,////job的abort任务完成

  //Producer:Job
  JOB_COMPLETED,//job完成
  JOB_FAIL_WAIT_TIMEDOUT,//job失败,因为等待超时

  //Producer:Any component
  JOB_DIAGNOSTIC_UPDATE,//job信息更新
  INTERNAL_ERROR,//job内部异常
  JOB_COUNTER_UPDATE,//job更新统计信息
  
  //Producer:TaskAttemptListener
  JOB_TASK_ATTEMPT_FETCH_FAILURE,//job尝试任务抓取失败事件,尝试任务抓取失败,因此该类会设置一个尝试任务ID,因为抓取都是在reduce中进行的,因此该任务可以指代reduce任务,会抓取map的尝试任务集合
  
  //Producer:RMContainerAllocator
  JOB_UPDATED_NODES,//更新节点信息
  JOB_AM_REBOOT//am重新启动
}
