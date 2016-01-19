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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;

abstract class StartEndTimesBase implements TaskRuntimeEstimator {
  static final float MINIMUM_COMPLETE_PROPORTION_TO_SPECULATE
      = 0.05F;//最小的完成比例,完成成功的比例必须大于一定比例,否则不需要进行再启动一个尝试任务
  static final int MINIMUM_COMPLETE_NUMBER_TO_SPECULATE
      = 1;//完成的任务数量必须大于最小值.否则不需要进行再启动一个尝试任务

  protected AppContext context = null;

  /**
   * 登记每一个TaskAttemptId尝试任务的时间
   * key为尝试任务ID,value为该任务的时间
   */
  protected final Map<TaskAttemptId, Long> startTimes = new ConcurrentHashMap<TaskAttemptId, Long>();

  // XXXX This class design assumes that the contents of AppContext.getAllJobs
  //   never changes.  Is that right?
  //
  // This assumption comes in in several places, mostly in data structure that
  //   can grow without limit if a AppContext gets new Job's when the old ones
  //   run out.  Also, these mapper statistics blocks won't cover the Job's
  //   we don't know about.
  //处理每一个job的map对应的统计值
  protected final Map<Job, DataStatistics> mapperStatistics = new HashMap<Job, DataStatistics>();

  //处理每一个job的reduce对应的统计值
  protected final Map<Job, DataStatistics> reducerStatistics = new HashMap<Job, DataStatistics>();

  private final Map<Job, Float> slowTaskRelativeTresholds = new HashMap<Job, Float>();

  //已经完成过的任务集合,没明白的地方,该集合会很大,没看到有方法情况该任务集合,应该是一个job执行完成后,会清空该job的任务的方法
  protected final Set<Task> doneTasks = new HashSet<Task>();

  //登记每一个尝试任务Id的时间
  @Override
  public void enrollAttempt(TaskAttemptStatus status, long timestamp) {
    startTimes.put(status.id,timestamp);
  }

  //获取每一个尝试任务的时间
  @Override
  public long attemptEnrolledTime(TaskAttemptId attemptID) {
    Long result = startTimes.get(attemptID);

    return result == null ? Long.MAX_VALUE : result;
  }


  /**
   * 初始化
   * 将所有的job中每一个map和reduce都设置成一个统计对象
   */
  @Override
  public void contextualize(Configuration conf, AppContext context) {
    this.context = context;

    Map<JobId, Job> allJobs = context.getAllJobs();

    for (Map.Entry<JobId, Job> entry : allJobs.entrySet()) {
      final Job job = entry.getValue();
      mapperStatistics.put(job, new DataStatistics());
      reducerStatistics.put(job, new DataStatistics());
      slowTaskRelativeTresholds.put
          (job, conf.getFloat(MRJobConfig.SPECULATIVE_SLOWTASK_THRESHOLD,1.0f));
    }
  }

  /**
   * 获取该任务ID对应的统计对象
   * 根据该任务的类型是Map还是Reduce获取属于该job的mao-reduce的统计值
   */
  protected DataStatistics dataStatisticsForTask(TaskId taskID) {
    JobId jobID = taskID.getJobId();
    Job job = context.getJob(jobID);

    if (job == null) {
      return null;
    }

    Task task = job.getTask(taskID);

    if (task == null) {
      return null;
    }

    return task.getType() == TaskType.MAP
            ? mapperStatistics.get(job)
            : task.getType() == TaskType.REDUCE
                ? reducerStatistics.get(job)
                : null;
  }

  /**
   * 入口
   */
  @Override
  public long thresholdRuntime(TaskId taskID) {
    JobId jobID = taskID.getJobId();
    Job job = context.getJob(jobID);

    TaskType type = taskID.getTaskType();

    //该任务的统计对象
    DataStatistics statistics = dataStatisticsForTask(taskID);
        
    //计算完成的任务数量
    int completedTasksOfType
        = type == TaskType.MAP
            ? job.getCompletedMaps() : job.getCompletedReduces();

    //计算总任务数
    int totalTasksOfType
        = type == TaskType.MAP
            ? job.getTotalMaps() : job.getTotalReduces();

    /**
     * 1.完成的任务数量必须大于最小值.否则不需要进行再启动一个尝试任务
     * 2.完成成功的比例必须大于一定比例,否则不需要进行再启动一个尝试任务
     */
    if (completedTasksOfType < MINIMUM_COMPLETE_NUMBER_TO_SPECULATE
        || (((float)completedTasksOfType) / totalTasksOfType)
              < MINIMUM_COMPLETE_PROPORTION_TO_SPECULATE ) {
      return Long.MAX_VALUE;
    }

    long result =  statistics == null
        ? Long.MAX_VALUE
        : (long)statistics.outlier(slowTaskRelativeTresholds.get(job));
    return result;
  }

  //获取该任务对应的统计完成时间的均值
  @Override
  public long estimatedNewAttemptRuntime(TaskId id) {
    DataStatistics statistics = dataStatisticsForTask(id);

    if (statistics == null) {
      return -1L;
    }

    return (long)statistics.mean();
  }

  @Override
  public void updateAttempt(TaskAttemptStatus status, long timestamp) {

    TaskAttemptId attemptID = status.id;
    TaskId taskID = attemptID.getTaskId();
    JobId jobID = taskID.getJobId();
    Job job = context.getJob(jobID);

    if (job == null) {
      return;
    }

    Task task = job.getTask(taskID);

    if (task == null) {
      return;
    }

    //获取该尝试任务的对应的以前设置的值
    Long boxedStart = startTimes.get(attemptID);
    long start = boxedStart == null ? Long.MIN_VALUE : boxedStart;
    
    TaskAttempt taskAttempt = task.getAttempt(attemptID);

    //如果该尝试任务已经执行完成
    if (taskAttempt.getState() == TaskAttemptState.SUCCEEDED) {
      boolean isNew = false;
      // is this a new success? 并且以前没有被执行完成
      synchronized (doneTasks) {
        if (!doneTasks.contains(task)) {
          doneTasks.add(task);
          isNew = true;
        }
      }

      // It's a new completion
      // Note that if a task completes twice [because of a previous speculation
      //  and a race, or a success followed by loss of the machine with the
      //  local data] we only count the first one.
      if (isNew) {//计算完成时间差,然后进行统计,获取该任务的平滑曲线,用该曲线来做判断是否要推迟执行
        long finish = timestamp;
        if (start > 1L && finish > 1L && start <= finish) {
          long duration = finish - start;

          DataStatistics statistics
          = dataStatisticsForTask(taskID);

          if (statistics != null) {
            statistics.add(duration);
          }
        }
      }
    }
  }
}
