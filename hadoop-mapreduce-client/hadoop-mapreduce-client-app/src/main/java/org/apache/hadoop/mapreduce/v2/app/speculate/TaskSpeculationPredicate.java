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

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;

/**
 * 任务的推测断定
 */
public class TaskSpeculationPredicate {
  //能否推测执行该任务,true表示该任务的尝试任务只有1个,则返回true,说明可以进行推测执行第二个任务
  boolean canSpeculate(AppContext context, TaskId taskID) {
    //这个类拒绝推测给任何已经被推测过任务的任务 或者不运行该任务,子类应该有更严格的限制
    // This class rejects speculating any task that already has speculations,
    //  or isn't running.
    //  Subclasses should call TaskSpeculationPredicate.canSpeculate(...) , but
    //  can be even more restrictive.
    JobId jobID = taskID.getJobId();
    Job job = context.getJob(jobID);
    Task task = job.getTask(taskID);
    //该任务的尝试任务只有1个,则返回true,说明可以进行推测执行第二个任务
    return task.getAttempts().size() == 1;
  }
}
