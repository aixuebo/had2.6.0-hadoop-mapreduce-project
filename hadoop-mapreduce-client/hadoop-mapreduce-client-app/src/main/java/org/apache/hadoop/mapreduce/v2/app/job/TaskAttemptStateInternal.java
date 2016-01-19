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

package org.apache.hadoop.mapreduce.v2.app.job;

import org.apache.hadoop.classification.InterfaceAudience.Private;

/**
* TaskAttemptImpl internal state machine states.
* 尝试任务的内部状态机
*/
@Private
public enum TaskAttemptStateInternal {
  NEW, 
  UNASSIGNED, //还未分配该尝试任务
  ASSIGNED, //已经分配了该尝试任务
  RUNNING, 
  COMMIT_PENDING, //该尝试任务等待提交
  
  SUCCESS_CONTAINER_CLEANUP, //被成功清理
  SUCCEEDED, 
  
  FAIL_CONTAINER_CLEANUP, //清理时候失败
  FAIL_TASK_CLEANUP, 
  FAILED, 
  
  KILL_CONTAINER_CLEANUP, 
  KILL_TASK_CLEANUP, 
  KILLED,
}