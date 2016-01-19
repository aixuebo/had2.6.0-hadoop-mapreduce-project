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

package org.apache.hadoop.mapreduce.v2.app.launcher;

import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

/**
 * 启动该尝试任务在一个容器里面
 * 事件是启动容器事件
 */
public class ContainerRemoteLaunchEvent extends ContainerLauncherEvent {

  private final Container allocatedContainer;//运行该尝试任务的容器
  private final ContainerLaunchContext containerLaunchContext;//运行该尝试任务的上下文环境
  private final Task task;//该尝试任务所对应的任务

  /**
   * 
   * @param taskAttemptID 尝试任务ID
   * @param containerLaunchContext 运行该尝试任务的上下文环境
   * @param allocatedContainer 运行该尝试任务的容器
   * @param remoteTask 该尝试任务所对应的任务
   */
  public ContainerRemoteLaunchEvent(TaskAttemptId taskAttemptID,
      ContainerLaunchContext containerLaunchContext,
      Container allocatedContainer, Task remoteTask) {
    super(taskAttemptID, allocatedContainer.getId(), StringInterner
      .weakIntern(allocatedContainer.getNodeId().toString()),
      allocatedContainer.getContainerToken(),
      ContainerLauncher.EventType.CONTAINER_REMOTE_LAUNCH);
    this.allocatedContainer = allocatedContainer;
    this.containerLaunchContext = containerLaunchContext;
    this.task = remoteTask;
  }

  public ContainerLaunchContext getContainerLaunchContext() {
    return this.containerLaunchContext;
  }

  public Container getAllocatedContainer() {
    return this.allocatedContainer;
  }

  public Task getRemoteTask() {
    return this.task;
  }
  
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }
}
