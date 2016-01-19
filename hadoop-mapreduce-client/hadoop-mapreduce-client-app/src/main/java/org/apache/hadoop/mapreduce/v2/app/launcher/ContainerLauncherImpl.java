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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This class is responsible for launching of containers.
 * ContainerLauncher.EventType事件处理类,处理容器被启动和清理尝试任务
 */
public class ContainerLauncherImpl extends AbstractService implements
    ContainerLauncher {

  static final Log LOG = LogFactory.getLog(ContainerLauncherImpl.class);

  //缓存容器对象的一个镜像,key是容器ID,value是容器对象
  private ConcurrentHashMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>(); 
  private final AppContext context;
  protected ThreadPoolExecutor launcherPool;//线程池对象
  protected static final int INITIAL_POOL_SIZE = 10;
  private int limitOnPoolSize;
  private Thread eventHandlingThread;//事件处理线程
  //事件队列,线程池处理该队列,该队列是阻塞队列
  protected BlockingQueue<ContainerLauncherEvent> eventQueue = new LinkedBlockingQueue<ContainerLauncherEvent>();
  private final AtomicBoolean stopped;
  private ContainerManagementProtocolProxy cmProxy;

  /**
   * 通过容器启动事件,获取对应的容器ID,从而获取到对应的容器对象
   */
  private Container getContainer(ContainerLauncherEvent event) {
    ContainerId id = event.getContainerID();
    Container c = containers.get(id);
    if(c == null) {//没有容器对象,则添加一个容器对象
      c = new Container(event.getTaskAttemptID(), event.getContainerID(),
          event.getContainerMgrAddress());
      Container old = containers.putIfAbsent(id, c);
      if(old != null) {
        c = old;
      }
    }
    return c;
  }
  
  //如果容器完成了,则删除该容器的镜像
  private void removeContainerIfDone(ContainerId id) {
    Container c = containers.get(id);
    if(c != null && c.isCompletelyDone()) {
      containers.remove(id);
    }
  }
  
  private static enum ContainerState {
    PREP, FAILED, RUNNING, DONE, KILLED_BEFORE_LAUNCH
  }

  /**
   * 每一个容器Id对应一个该容器对象的镜像对象
   */
  private class Container {
    private ContainerState state;//容器状态,默认是准备阶段
    // store enough information to be able to cleanup the container
    private TaskAttemptId taskAttemptID;//该容器对应的尝试任务ID
    private ContainerId containerID;//容器ID
    final private String containerMgrAddress;//该容器所在节点http
    
    /**
     * 
     * @param taId 该容器对应的尝试任务ID
     * @param containerID 容器ID
     * @param containerMgrAddress 该容器所在节点http
     */
    public Container(TaskAttemptId taId, ContainerId containerID,
        String containerMgrAddress) {
      this.state = ContainerState.PREP;
      this.taskAttemptID = taId;
      this.containerMgrAddress = containerMgrAddress;
      this.containerID = containerID;
    }
    
    //判断该任务是否完成
    public synchronized boolean isCompletelyDone() {
      return state == ContainerState.DONE || state == ContainerState.FAILED;
    }
    
    /**
     * 启动容器
     */
    @SuppressWarnings("unchecked")
    public synchronized void launch(ContainerRemoteLaunchEvent event) {
      LOG.info("Launching " + taskAttemptID);
      if(this.state == ContainerState.KILLED_BEFORE_LAUNCH) {
        state = ContainerState.DONE;
        sendContainerLaunchFailedMsg(taskAttemptID, 
            "Container was killed before it was launched");
        return;
      }
      
      ContainerManagementProtocolProxyData proxy = null;
      try {

        proxy = getCMProxy(containerMgrAddress, containerID);

        // Construct the actual Container 启动容器的上下文
        ContainerLaunchContext containerLaunchContext =
          event.getContainerLaunchContext();

        // Now launch the actual container
        StartContainerRequest startRequest =
            StartContainerRequest.newInstance(containerLaunchContext,
              event.getContainerToken());
        List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
        list.add(startRequest);
        StartContainersRequest requestList = StartContainersRequest.newInstance(list);
        StartContainersResponse response =
            proxy.getContainerManagementProtocol().startContainers(requestList);
        if (response.getFailedRequests() != null
            && response.getFailedRequests().containsKey(containerID)) {
          throw response.getFailedRequests().get(containerID).deSerialize();
        }
        ByteBuffer portInfo =
            response.getAllServicesMetaData().get(
                ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID);
        int port = -1;
        if(portInfo != null) {
          port = ShuffleHandler.deserializeMetaData(portInfo);
        }
        LOG.info("Shuffle port returned by ContainerManager for "
            + taskAttemptID + " : " + port);

        if(port < 0) {
          this.state = ContainerState.FAILED;
          throw new IllegalStateException("Invalid shuffle port number "
              + port + " returned for " + taskAttemptID);
        }

        // after launching, send launched event to task attempt to move
        // it from ASSIGNED to RUNNING state
        context.getEventHandler().handle(
            new TaskAttemptContainerLaunchedEvent(taskAttemptID, port));
        this.state = ContainerState.RUNNING;
      } catch (Throwable t) {
        String message = "Container launch failed for " + containerID + " : "
            + StringUtils.stringifyException(t);
        this.state = ContainerState.FAILED;
        sendContainerLaunchFailedMsg(taskAttemptID, message);
      } finally {
        if (proxy != null) {
          cmProxy.mayBeCloseProxy(proxy);
        }
      }
    }
    
    /**
     * 清理容器,或者将容器kill掉
     */
    @SuppressWarnings("unchecked")
    public synchronized void kill() {

      if(this.state == ContainerState.PREP) {
        this.state = ContainerState.KILLED_BEFORE_LAUNCH;
      } else if (!isCompletelyDone()) {
        LOG.info("KILLING " + taskAttemptID);

        ContainerManagementProtocolProxyData proxy = null;
        try {
          proxy = getCMProxy(this.containerMgrAddress, this.containerID);

          // kill the remote container if already launched
          List<ContainerId> ids = new ArrayList<ContainerId>();
          ids.add(this.containerID);
          StopContainersRequest request = StopContainersRequest.newInstance(ids);
          StopContainersResponse response =
              proxy.getContainerManagementProtocol().stopContainers(request);
          if (response.getFailedRequests() != null
              && response.getFailedRequests().containsKey(this.containerID)) {
            throw response.getFailedRequests().get(this.containerID)
              .deSerialize();
          }
        } catch (Throwable t) {
          // ignore the cleanup failure
          String message = "cleanup failed for container "
              + this.containerID + " : "
              + StringUtils.stringifyException(t);
          context.getEventHandler()
              .handle(
                  new TaskAttemptDiagnosticsUpdateEvent(this.taskAttemptID,
                      message));
          LOG.warn(message);
        } finally {
          if (proxy != null) {
            cmProxy.mayBeCloseProxy(proxy);
          }
        }
        this.state = ContainerState.DONE;
      }
      // after killing, send killed event to task attempt
      context.getEventHandler().handle(
          new TaskAttemptEvent(this.taskAttemptID,
              TaskAttemptEventType.TA_CONTAINER_CLEANED));
    }
  }

  public ContainerLauncherImpl(AppContext context) {
    super(ContainerLauncherImpl.class.getName());
    this.context = context;
    this.stopped = new AtomicBoolean(false);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.limitOnPoolSize = conf.getInt(
        MRJobConfig.MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT,
        MRJobConfig.DEFAULT_MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT);
    LOG.info("Upper limit on the thread pool size is " + this.limitOnPoolSize);
    super.serviceInit(conf);
    cmProxy = new ContainerManagementProtocolProxy(conf);
  }

  protected void serviceStart() throws Exception {

    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(
        "ContainerLauncher #%d").setDaemon(true).build();

    // Start with a default core-pool size of 10 and change it dynamically.线程池
    launcherPool = new ThreadPoolExecutor(INITIAL_POOL_SIZE,
        Integer.MAX_VALUE, 1, TimeUnit.HOURS,
        new LinkedBlockingQueue<Runnable>(),
        tf);
    
    
    /**
     * 线程
     * 1.从eventQueue队列中获取一个待启动的事件对象,该队列是阻塞队列
     * 2.
     */
    eventHandlingThread = new Thread() {
      @Override
      public void run() {
        ContainerLauncherEvent event = null;
        //正在处理的容器集合中所在的节点集合
        Set<String> allNodes = new HashSet<String>();

        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            //从eventQueue队列中获取一个待启动的事件对象,该队列是阻塞队列
            event = eventQueue.take();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.error("Returning, interrupted : " + e);
            }
            return;
          }
          //添加节点集合
          allNodes.add(event.getContainerMgrAddress());

          int poolSize = launcherPool.getCorePoolSize();

          // See if we need up the pool size only if haven't reached the
          // maximum limit yet.扩容线程池
          if (poolSize != limitOnPoolSize) {

            // nodes where containers will run at *this* point of time. This is
            // *not* the cluster size and doesn't need to be.
            int numNodes = allNodes.size();
            int idealPoolSize = Math.min(limitOnPoolSize, numNodes);

            if (poolSize < idealPoolSize) {
              // Bump up the pool size to idealPoolSize+INITIAL_POOL_SIZE, the
              // later is just a buffer so we are not always increasing the
              // pool-size
              int newPoolSize = Math.min(limitOnPoolSize, idealPoolSize
                  + INITIAL_POOL_SIZE);
              LOG.info("Setting ContainerLauncher pool size to " + newPoolSize
                  + " as number-of-nodes to talk to is " + numNodes);
              launcherPool.setCorePoolSize(newPoolSize);
            }
          }

          // the events from the queue are handled in parallel
          // using a thread pool用线程池处理该事件
          launcherPool.execute(createEventProcessor(event));

          // TODO: Group launching of multiple containers to a single
          // NodeManager into a single connection
        }
      }
    };
    eventHandlingThread.setName("ContainerLauncher Event Handler");
    eventHandlingThread.start();
    super.serviceStart();
  }

  //所有运行的容器都kill掉
  private void shutdownAllContainers() {
    for (Container ct : this.containers.values()) {
      if (ct != null) {
        ct.kill();
      }
    }
  }

  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    // shutdown any containers that might be left running
    shutdownAllContainers();
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    if (launcherPool != null) {
      launcherPool.shutdownNow();
    }
    super.serviceStop();
  }

  /**
   * 为每一个事件分配一个线程去处理
   */
  protected EventProcessor createEventProcessor(ContainerLauncherEvent event) {
    return new EventProcessor(event);
  }

  /**
   * Setup and start the container on remote nodemanager.
   * 为每一个事件分配一个线程去处理
   */
  class EventProcessor implements Runnable {
    private ContainerLauncherEvent event;

    EventProcessor(ContainerLauncherEvent event) {
      this.event = event;
    }

    @Override
    public void run() {
      LOG.info("Processing the event " + event.toString());

      // Load ContainerManager tokens before creating a connection.
      // TODO: Do it only once per NodeManager.获取容器ID
      ContainerId containerID = event.getContainerID();

      //获取对应的容器镜像
      Container c = getContainer(event);
      //根据事件类型处理不同逻辑
      switch(event.getType()) {

      case CONTAINER_REMOTE_LAUNCH:
        ContainerRemoteLaunchEvent launchEvent
            = (ContainerRemoteLaunchEvent) event;
        c.launch(launchEvent);
        break;

      case CONTAINER_REMOTE_CLEANUP:
        c.kill();
        break;
      }
      removeContainerIfDone(containerID);
    }
  }
  
  @SuppressWarnings("unchecked")
  void sendContainerLaunchFailedMsg(TaskAttemptId taskAttemptID,
      String message) {
    LOG.error(message);
    context.getEventHandler().handle(
        new TaskAttemptDiagnosticsUpdateEvent(taskAttemptID, message));
    context.getEventHandler().handle(
        new TaskAttemptEvent(taskAttemptID,
            TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED));
  }

  //将事件添加到队列中
  @Override
  public void handle(ContainerLauncherEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }
  
  public ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
      getCMProxy(String containerMgrBindAddr, ContainerId containerId)
          throws IOException {
    return cmProxy.getProxy(containerMgrBindAddr, containerId);
  }
}
