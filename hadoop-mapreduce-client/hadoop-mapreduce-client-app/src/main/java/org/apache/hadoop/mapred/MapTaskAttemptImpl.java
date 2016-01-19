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

package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;

@SuppressWarnings("rawtypes")
public class MapTaskAttemptImpl extends TaskAttemptImpl {

  private final TaskSplitMetaInfo splitInfo;

  /**
   * 
   * @param taskId 该map尝试任务属于哪个任务
   * @param attempt 该任务的尝试任务序号,每个任务从0开始算
   * @param eventHandler
   * @param jobFile job的配置文件/tmp/hadoop-yarn/staging/user/.staging/jobIdString/job.xml
   * @param partition 该map任务的序号
   * @param splitInfo 该map处理的split数据源文件
   * @param conf
   * @param taskAttemptListener
   * @param jobToken
   * @param credentials
   * @param clock
   * @param appContext
   */
  public MapTaskAttemptImpl(TaskId taskId, int attempt, 
      EventHandler eventHandler, Path jobFile, 
      int partition, TaskSplitMetaInfo splitInfo, JobConf conf,
      TaskAttemptListener taskAttemptListener, 
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      AppContext appContext) {
    super(taskId, attempt, eventHandler, 
        taskAttemptListener, jobFile, partition, conf, splitInfo.getLocations(),
        jobToken, credentials, clock, appContext);
    this.splitInfo = splitInfo;
  }

  @Override
  public Task createRemoteTask() {
    //job file name is set in TaskAttempt, setting it null here
    MapTask mapTask =
      new MapTask("", TypeConverter.fromYarn(getID()), partition,
          splitInfo.getSplitIndex(), 1); // YARN doesn't have the concept of slots per task, set it as 1.
    mapTask.setUser(conf.get(MRJobConfig.USER_NAME));
    mapTask.setConf(conf);
    return mapTask;
  }

}
