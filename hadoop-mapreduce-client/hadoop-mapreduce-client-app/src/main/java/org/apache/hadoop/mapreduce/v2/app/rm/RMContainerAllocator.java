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

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.NormalizedResourceEvent;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobUpdatedNodesEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

/**
 * Allocates the container from the ResourceManager scheduler.
 * 实现ContainerAllocator.EventType类型的事件处理
 */
public class RMContainerAllocator extends RMContainerRequestor
    implements ContainerAllocator {

  static final Log LOG = LogFactory.getLog(RMContainerAllocator.class);
  
  public static final 
  float DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART = 0.05f;
  
  private static final Priority PRIORITY_FAST_FAIL_MAP;//优先级是5
  private static final Priority PRIORITY_REDUCE;//优先级10
  private static final Priority PRIORITY_MAP;//优先级20

  //reduce任务资源被抢占,给map尝试任务使用 的提示信息
  @VisibleForTesting
  public static final String RAMPDOWN_DIAGNOSTIC = "Reducer preempted "
      + "to make room for pending map attempts";

  private Thread eventHandlingThread;
  private final AtomicBoolean stopped;

  static {
    PRIORITY_FAST_FAIL_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_FAST_FAIL_MAP.setPriority(5);
    PRIORITY_REDUCE = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_REDUCE.setPriority(10);
    PRIORITY_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_MAP.setPriority(20);
  }
  
  /*
  Vocabulary Used: 
  pending -> requests which are NOT yet sent to RM
  scheduled -> requests which are sent to RM but not yet assigned
  assigned -> requests which are assigned to a container
  completed -> request corresponding to which container has completed
  
  Lifecycle of map
  scheduled->assigned->completed
  
  Lifecycle of reduce
  pending->scheduled->assigned->completed
  
  Maps are scheduled as soon as their requests are received. Reduces are 
  added to the pending and are ramped up (added to scheduled) based 
  on completed maps and current availability in the cluster.
  */
  
  //reduces which are not yet scheduled reduce容器尚未被调度的集合
  private final LinkedList<ContainerRequest> pendingReduces = new LinkedList<ContainerRequest>(); 

  //holds information about the assigned containers to task attempts
  private final AssignedRequests assignedRequests = new AssignedRequests();
  
  //holds scheduled requests to be fulfilled by RM
  private final ScheduledRequests scheduledRequests = new ScheduledRequests();
  
  private int containersAllocated = 0;//被获取的容器数量
  private int containersReleased = 0;//容器被释放的数量
  private int hostLocalAssigned = 0;
  private int rackLocalAssigned = 0;
  private int lastCompletedTasks = 0;
  
  private boolean recalculateReduceSchedule = false;
  private Resource mapResourceRequest = Resources.none();//设置map需要的请求资源
  private Resource reduceResourceRequest = Resources.none();//设置reduce需要的请求资源
  
  private boolean reduceStarted = false;
  private float maxReduceRampupLimit = 0;
  private float maxReducePreemptionLimit = 0;
  /**
   * after this threshold, if the container request is not allocated, it is
   * considered delayed.
   */
  private long allocationDelayThresholdMs = 0;
  private float reduceSlowStart = 0;
  private long retryInterval;
  private long retrystartTime;
  private Clock clock;

  //事件的阻塞队列
  @VisibleForTesting
  protected BlockingQueue<ContainerAllocatorEvent> eventQueue = new LinkedBlockingQueue<ContainerAllocatorEvent>();
    

  private ScheduleStats scheduleStats = new ScheduleStats();

  public RMContainerAllocator(ClientService clientService, AppContext context) {
    super(clientService, context);
    this.stopped = new AtomicBoolean(false);
    this.clock = context.getClock();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    reduceSlowStart = conf.getFloat(
        MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 
        DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART);
    maxReduceRampupLimit = conf.getFloat(
        MRJobConfig.MR_AM_JOB_REDUCE_RAMPUP_UP_LIMIT, 
        MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_RAMP_UP_LIMIT);
    maxReducePreemptionLimit = conf.getFloat(
        MRJobConfig.MR_AM_JOB_REDUCE_PREEMPTION_LIMIT,
        MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_PREEMPTION_LIMIT);
    allocationDelayThresholdMs = conf.getInt(
        MRJobConfig.MR_JOB_REDUCER_PREEMPT_DELAY_SEC,
        MRJobConfig.DEFAULT_MR_JOB_REDUCER_PREEMPT_DELAY_SEC) * 1000;//sec -> ms
    RackResolver.init(conf);
    retryInterval = getConfig().getLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS,
                                MRJobConfig.DEFAULT_MR_AM_TO_RM_WAIT_INTERVAL_MS);
    // Init startTime to current time. If all goes well, it will be reset after
    // first attempt to contact RM.
    retrystartTime = System.currentTimeMillis();
  }

  @Override
  protected void serviceStart() throws Exception {
    this.eventHandlingThread = new Thread() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {

        ContainerAllocatorEvent event;

        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            event = RMContainerAllocator.this.eventQueue.take();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.error("Returning, interrupted : " + e);
            }
            return;
          }

          try {
            handleEvent(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " to the ContainreAllocator", t);
            // Kill the AM
            eventHandler.handle(new JobEvent(getJob().getID(),
              JobEventType.INTERNAL_ERROR));
            return;
          }
        }
      }
    };
    this.eventHandlingThread.start();
    super.serviceStart();
  }

  @Override
  protected synchronized void heartbeat() throws Exception {
    scheduleStats.updateAndLogIfChanged("Before Scheduling: ");
    List<Container> allocatedContainers = getResources();
    if (allocatedContainers != null && allocatedContainers.size() > 0) {
      scheduledRequests.assign(allocatedContainers);
    }

    int completedMaps = getJob().getCompletedMaps();
    int completedTasks = completedMaps + getJob().getCompletedReduces();
    if ((lastCompletedTasks != completedTasks) ||
          (scheduledRequests.maps.size() > 0)) {
      lastCompletedTasks = completedTasks;
      recalculateReduceSchedule = true;
    }

    if (recalculateReduceSchedule) {
      preemptReducesIfNeeded();
      scheduleReduces(
          getJob().getTotalMaps(), completedMaps,
          scheduledRequests.maps.size(), scheduledRequests.reduces.size(), 
          assignedRequests.maps.size(), assignedRequests.reduces.size(),
          mapResourceRequest, reduceResourceRequest,
          pendingReduces.size(), 
          maxReduceRampupLimit, reduceSlowStart);
      recalculateReduceSchedule = false;
    }

    scheduleStats.updateAndLogIfChanged("After Scheduling: ");
  }

  @Override
  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    super.serviceStop();
    scheduleStats.log("Final Stats: ");
  }

  @Private
  @VisibleForTesting
  AssignedRequests getAssignedRequests() {
    return assignedRequests;
  }

  @Private
  @VisibleForTesting
  ScheduledRequests getScheduledRequests() {
    return scheduledRequests;
  }

  public boolean getIsReduceStarted() {
    return reduceStarted;
  }
  
  public void setIsReduceStarted(boolean reduceStarted) {
    this.reduceStarted = reduceStarted; 
  }

  @Override
  public void handle(ContainerAllocatorEvent event) {
    int qSize = eventQueue.size();
    //每隔1000个,就打印一个日志
    if (qSize != 0 && qSize % 1000 == 0) {
      LOG.info("Size of event-queue in RMContainerAllocator is " + qSize);
    }
    int remCapacity = eventQueue.remainingCapacity();
    if (remCapacity < 1000) {
      LOG.warn("Very low remaining capacity in the event-queue "
          + "of RMContainerAllocator: " + remCapacity);
    }
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }

  @SuppressWarnings({ "unchecked" })
  protected synchronized void handleEvent(ContainerAllocatorEvent event) {
    recalculateReduceSchedule = true;
    if (event.getType() == ContainerAllocator.EventType.CONTAINER_REQ) {
      ContainerRequestEvent reqEvent = (ContainerRequestEvent) event;
      JobId jobId = getJob().getID();
      Resource supportedMaxContainerCapability = getMaxContainerCapability();
      if (reqEvent.getAttemptID().getTaskId().getTaskType().equals(TaskType.MAP)) {//map类型的任务容器请求
        if (mapResourceRequest.equals(Resources.none())) {
          mapResourceRequest = reqEvent.getCapability();//设置map需要的请求资源
          //日志记录一个map或者reduce的一个任务资源所占用的内存数
          eventHandler.handle(new JobHistoryEvent(jobId,
            new NormalizedResourceEvent(
              org.apache.hadoop.mapreduce.TaskType.MAP, mapResourceRequest
                .getMemory())));
          LOG.info("mapResourceRequest:" + mapResourceRequest);
          //校验是否超出最大资源限制
          if (mapResourceRequest.getMemory() > supportedMaxContainerCapability
            .getMemory()
              || mapResourceRequest.getVirtualCores() > supportedMaxContainerCapability
                .getVirtualCores()) {
            String diagMsg =
                "MAP capability required is more than the supported "
                    + "max container capability in the cluster. Killing the Job. mapResourceRequest: "
                    + mapResourceRequest + " maxContainerCapability:"
                    + supportedMaxContainerCapability;
            LOG.info(diagMsg);
            eventHandler.handle(new JobDiagnosticsUpdateEvent(jobId, diagMsg));
            //kill掉该job
            eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
          }
        }
        // set the resources
        reqEvent.getCapability().setMemory(mapResourceRequest.getMemory());
        reqEvent.getCapability().setVirtualCores(
          mapResourceRequest.getVirtualCores());
        scheduledRequests.addMap(reqEvent);//maps are immediately scheduled
      } else {
        if (reduceResourceRequest.equals(Resources.none())) {
          reduceResourceRequest = reqEvent.getCapability();
          eventHandler.handle(new JobHistoryEvent(jobId,
            new NormalizedResourceEvent(
              org.apache.hadoop.mapreduce.TaskType.REDUCE,
              reduceResourceRequest.getMemory())));
          LOG.info("reduceResourceRequest:" + reduceResourceRequest);
          //校验reduce需要的容器是否超出了最大限制
          if (reduceResourceRequest.getMemory() > supportedMaxContainerCapability
            .getMemory()
              || reduceResourceRequest.getVirtualCores() > supportedMaxContainerCapability
                .getVirtualCores()) {
            String diagMsg =
                "REDUCE capability required is more than the "
                    + "supported max container capability in the cluster. Killing the "
                    + "Job. reduceResourceRequest: " + reduceResourceRequest
                    + " maxContainerCapability:"
                    + supportedMaxContainerCapability;
            LOG.info(diagMsg);
            eventHandler.handle(new JobDiagnosticsUpdateEvent(jobId, diagMsg));
            eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
          }
        }
        // set the resources
        reqEvent.getCapability().setMemory(reduceResourceRequest.getMemory());
        reqEvent.getCapability().setVirtualCores(
          reduceResourceRequest.getVirtualCores());
        if (reqEvent.getEarlierAttemptFailed()) {
          //add to the front of queue for fail fast
          pendingReduces.addFirst(new ContainerRequest(reqEvent, PRIORITY_REDUCE));
        } else {
          pendingReduces.add(new ContainerRequest(reqEvent, PRIORITY_REDUCE));
          //reduces are added to pending and are slowly ramped up
        }
      }
      
    } else if (
        event.getType() == ContainerAllocator.EventType.CONTAINER_DEALLOCATE) {
  
      LOG.info("Processing the event " + event.toString());

      TaskAttemptId aId = event.getAttemptID();
      
      boolean removed = scheduledRequests.remove(aId);
      if (!removed) {
        ContainerId containerId = assignedRequests.get(aId);
        if (containerId != null) {
          removed = true;
          assignedRequests.remove(aId);
          containersReleased++;
          pendingRelease.add(containerId);
          release(containerId);
        }
      }
      if (!removed) {
        LOG.error("Could not deallocate container for task attemptId " + 
            aId);
      }
    } else if (
        event.getType() == ContainerAllocator.EventType.CONTAINER_FAILED) {
      //计算该节点失败次数,然后判断是否加入到黑名单中
      ContainerFailedEvent fEv = (ContainerFailedEvent) event;
      String host = getHost(fEv.getContMgrAddress());
      containerFailedOnHost(host);
    }
  }

  //获取host,即:之前的数据
  private static String getHost(String contMgrAddress) {
    String host = contMgrAddress;
    String[] hostport = host.split(":");
    if (hostport.length == 2) {
      host = hostport[0];
    }
    return host;
  }

  //设置reduce需要的请求资源
  @Private
  @VisibleForTesting
  synchronized void setReduceResourceRequest(Resource res) {
    this.reduceResourceRequest = res;
  }

  //设置map需要的请求资源
  @Private
  @VisibleForTesting
  synchronized void setMapResourceRequest(Resource res) {
    this.mapResourceRequest = res;
  }

  @Private
  @VisibleForTesting
  void preemptReducesIfNeeded() {
    if (reduceResourceRequest.equals(Resources.none())) {
      return; // no reduces
    }
    //check if reduces have taken over the whole cluster and there are 
    //unassigned maps
    if (scheduledRequests.maps.size() > 0) {
      Resource resourceLimit = getResourceLimit();
      Resource availableResourceForMap =
          Resources.subtract(
            resourceLimit,
            Resources.multiply(reduceResourceRequest,
              assignedRequests.reduces.size()
                  - assignedRequests.preemptionWaitingReduces.size()));
      // availableMemForMap must be sufficient to run at least 1 map
      if (ResourceCalculatorUtils.computeAvailableContainers(availableResourceForMap,
        mapResourceRequest, getSchedulerResourceTypes()) <= 0) {
        // to make sure new containers are given to maps and not reduces
        // ramp down all scheduled reduces if any
        // (since reduces are scheduled at higher priority than maps)
        LOG.info("Ramping down all scheduled reduces:"
            + scheduledRequests.reduces.size());
        for (ContainerRequest req : scheduledRequests.reduces.values()) {
          pendingReduces.add(req);
        }
        scheduledRequests.reduces.clear();
 
        //do further checking to find the number of map requests that were
        //hanging around for a while
        int hangingMapRequests = getNumOfHangingRequests(scheduledRequests.maps);
        if (hangingMapRequests > 0) {
          // preempt for making space for at least one map
          int preemptionReduceNumForOneMap =
              ResourceCalculatorUtils.divideAndCeilContainers(mapResourceRequest,
                reduceResourceRequest, getSchedulerResourceTypes());
          int preemptionReduceNumForPreemptionLimit =
              ResourceCalculatorUtils.divideAndCeilContainers(
                Resources.multiply(resourceLimit, maxReducePreemptionLimit),
                reduceResourceRequest, getSchedulerResourceTypes());
          int preemptionReduceNumForAllMaps =
              ResourceCalculatorUtils.divideAndCeilContainers(
                Resources.multiply(mapResourceRequest, hangingMapRequests),
                reduceResourceRequest, getSchedulerResourceTypes());
          int toPreempt =
              Math.min(Math.max(preemptionReduceNumForOneMap,
                preemptionReduceNumForPreemptionLimit),
                preemptionReduceNumForAllMaps);

          LOG.info("Going to preempt " + toPreempt
              + " due to lack of space for maps");
          assignedRequests.preemptReduce(toPreempt);
        }
      }
    }
  }
 
  private int getNumOfHangingRequests(Map<TaskAttemptId, ContainerRequest> requestMap) {
    if (allocationDelayThresholdMs <= 0)
      return requestMap.size();
    int hangingRequests = 0;
    long currTime = clock.getTime();
    for (ContainerRequest request: requestMap.values()) {
      long delay = currTime - request.requestTimeMs;
      if (delay > allocationDelayThresholdMs)
        hangingRequests++;
    }
    return hangingRequests;
  }
  
  @Private
  public void scheduleReduces(
      int totalMaps, int completedMaps,
      int scheduledMaps, int scheduledReduces,
      int assignedMaps, int assignedReduces,
      Resource mapResourceReqt, Resource reduceResourceReqt,
      int numPendingReduces,
      float maxReduceRampupLimit, float reduceSlowStart) {
    
    if (numPendingReduces == 0) {
      return;
    }
    
    // get available resources for this job
    Resource headRoom = getAvailableResources();
    if (headRoom == null) {
      headRoom = Resources.none();
    }

    LOG.info("Recalculating schedule, headroom=" + headRoom);
    
    //check for slow start
    if (!getIsReduceStarted()) {//not set yet
      int completedMapsForReduceSlowstart = (int)Math.ceil(reduceSlowStart * 
                      totalMaps);
      if(completedMaps < completedMapsForReduceSlowstart) {
        LOG.info("Reduce slow start threshold not met. " +
              "completedMapsForReduceSlowstart " + 
            completedMapsForReduceSlowstart);
        return;
      } else {
        LOG.info("Reduce slow start threshold reached. Scheduling reduces.");
        setIsReduceStarted(true);
      }
    }
    
    //if all maps are assigned, then ramp up all reduces irrespective of the
    //headroom
    if (scheduledMaps == 0 && numPendingReduces > 0) {
      LOG.info("All maps assigned. " +
          "Ramping up all remaining reduces:" + numPendingReduces);
      scheduleAllReduces();
      return;
    }

    float completedMapPercent = 0f;
    if (totalMaps != 0) {//support for 0 maps
      completedMapPercent = (float)completedMaps/totalMaps;
    } else {
      completedMapPercent = 1;
    }
    
    Resource netScheduledMapResource =
        Resources.multiply(mapResourceReqt, (scheduledMaps + assignedMaps));

    Resource netScheduledReduceResource =
        Resources.multiply(reduceResourceReqt,
          (scheduledReduces + assignedReduces));

    Resource finalMapResourceLimit;
    Resource finalReduceResourceLimit;

    // ramp up the reduces based on completed map percentage
    Resource totalResourceLimit = getResourceLimit();

    Resource idealReduceResourceLimit =
        Resources.multiply(totalResourceLimit,
          Math.min(completedMapPercent, maxReduceRampupLimit));
    Resource ideaMapResourceLimit =
        Resources.subtract(totalResourceLimit, idealReduceResourceLimit);

    // check if there aren't enough maps scheduled, give the free map capacity
    // to reduce.
    // Even when container number equals, there may be unused resources in one
    // dimension
    if (ResourceCalculatorUtils.computeAvailableContainers(ideaMapResourceLimit,
      mapResourceReqt, getSchedulerResourceTypes()) >= (scheduledMaps + assignedMaps)) {
      // enough resource given to maps, given the remaining to reduces
      Resource unusedMapResourceLimit =
          Resources.subtract(ideaMapResourceLimit, netScheduledMapResource);
      finalReduceResourceLimit =
          Resources.add(idealReduceResourceLimit, unusedMapResourceLimit);
      finalMapResourceLimit =
          Resources.subtract(totalResourceLimit, finalReduceResourceLimit);
    } else {
      finalMapResourceLimit = ideaMapResourceLimit;
      finalReduceResourceLimit = idealReduceResourceLimit;
    }

    LOG.info("completedMapPercent " + completedMapPercent
        + " totalResourceLimit:" + totalResourceLimit
        + " finalMapResourceLimit:" + finalMapResourceLimit
        + " finalReduceResourceLimit:" + finalReduceResourceLimit
        + " netScheduledMapResource:" + netScheduledMapResource
        + " netScheduledReduceResource:" + netScheduledReduceResource);

    int rampUp =
        ResourceCalculatorUtils.computeAvailableContainers(Resources.subtract(
                finalReduceResourceLimit, netScheduledReduceResource),
            reduceResourceReqt, getSchedulerResourceTypes());

    if (rampUp > 0) {
      rampUp = Math.min(rampUp, numPendingReduces);
      LOG.info("Ramping up " + rampUp);
      rampUpReduces(rampUp);
    } else if (rampUp < 0) {
      int rampDown = -1 * rampUp;
      rampDown = Math.min(rampDown, scheduledReduces);
      LOG.info("Ramping down " + rampDown);
      rampDownReduces(rampDown);
    }
  }

  @Private
  public void scheduleAllReduces() {
    for (ContainerRequest req : pendingReduces) {
      scheduledRequests.addReduce(req);
    }
    pendingReduces.clear();
  }
  
  @Private
  public void rampUpReduces(int rampUp) {
    //more reduce to be scheduled
    for (int i = 0; i < rampUp; i++) {
      ContainerRequest request = pendingReduces.removeFirst();
      scheduledRequests.addReduce(request);
    }
  }
  
  @Private
  public void rampDownReduces(int rampDown) {
    //remove from the scheduled and move back to pending
    for (int i = 0; i < rampDown; i++) {
      ContainerRequest request = scheduledRequests.removeReduce();
      pendingReduces.add(request);
    }
  }
  
  /**
   * 请求资源,返回申请到的容器集合
   */
  @SuppressWarnings("unchecked")
  private List<Container> getResources() throws Exception {
    // will be null the first time 第一次的时候会是null
    //getAvailableResources方法返回 每次请求后,返回值中报告的集群的可用资源集合
    Resource headRoom =
        getAvailableResources() == null ? Resources.none() :
            Resources.clone(getAvailableResources());//当前集群可用资源
    AllocateResponse response;
    /*
     * If contact with RM is lost, the AM will wait MR_AM_TO_RM_WAIT_INTERVAL_MS
     * milliseconds before aborting. During this interval, AM will still try
     * to contact the RM.
     */
    try {
      response = makeRemoteRequest();//请求资源,并且返回对象
      // Reset retry count if no exception occurred.
      retrystartTime = System.currentTimeMillis();
    } catch (ApplicationAttemptNotFoundException e ) {
      // This can happen if the RM has been restarted. If it is in that state,
      // this application must clean itself up.
      eventHandler.handle(new JobEvent(this.getJob().getID(),
        JobEventType.JOB_AM_REBOOT));
      throw new YarnRuntimeException(
        "Resource Manager doesn't recognize AttemptId: "
            + this.getContext().getApplicationID(), e);
    } catch (ApplicationMasterNotRegisteredException e) {
      LOG.info("ApplicationMaster is out of sync with ResourceManager,"
          + " hence resync and send outstanding requests.");
      // RM may have restarted, re-register with RM.
      lastResponseID = 0;
      register();
      addOutstandingRequestOnResync();
      return null;
    } catch (Exception e) {
      // This can happen when the connection to the RM has gone down. Keep
      // re-trying until the retryInterval has expired.
      if (System.currentTimeMillis() - retrystartTime >= retryInterval) {
        LOG.error("Could not contact RM after " + retryInterval + " milliseconds.");
        eventHandler.handle(new JobEvent(this.getJob().getID(),
                                         JobEventType.INTERNAL_ERROR));
        throw new YarnRuntimeException("Could not contact RM after " +
                                retryInterval + " milliseconds.");
      }
      // Throw this up to the caller, which may decide to ignore it and
      // continue to attempt to contact the RM.
      throw e;
    }
    //请求资源后,新的当前集群可用资源
    Resource newHeadRoom =
        getAvailableResources() == null ? Resources.none()
            : getAvailableResources();
    List<Container> newContainers = response.getAllocatedContainers();
    // Setting NMTokens
    if (response.getNMTokens() != null) {
      for (NMToken nmToken : response.getNMTokens()) {
        NMTokenCache.setNMToken(nmToken.getNodeId().toString(),
            nmToken.getToken());
      }
    }

    // Setting AMRMToken
    if (response.getAMRMToken() != null) {
      updateAMRMToken(response.getAMRMToken());
    }

    List<ContainerStatus> finishedContainers = response.getCompletedContainersStatuses();
    if (newContainers.size() + finishedContainers.size() > 0
        || !headRoom.equals(newHeadRoom)) {
      //something changed
      recalculateReduceSchedule = true;
      if (LOG.isDebugEnabled() && !headRoom.equals(newHeadRoom)) {
        LOG.debug("headroom=" + newHeadRoom);
      }
    }

    if (LOG.isDebugEnabled()) {
      for (Container cont : newContainers) {
        LOG.debug("Received new Container :" + cont);
      }
    }

    //Called on each allocation. Will know about newly blacklisted/added hosts.
    computeIgnoreBlacklisting();

    //更新节点信息,以及将失效的节点上的容器尝试任务kill掉
    handleUpdatedNodes(response);

    //处理完成的容器
    for (ContainerStatus cont : finishedContainers) {
      LOG.info("Received completed container " + cont.getContainerId());
      TaskAttemptId attemptID = assignedRequests.get(cont.getContainerId());
      if (attemptID == null) {
        LOG.error("Container complete event for unknown container id "
            + cont.getContainerId());
      } else {
        pendingRelease.remove(cont.getContainerId());
        assignedRequests.remove(attemptID);
        
        // send the container completed event to Task attempt
        eventHandler.handle(createContainerFinishedEvent(cont, attemptID));
        
        // Send the diagnostics
        String diagnostics = StringInterner.weakIntern(cont.getDiagnostics());
        eventHandler.handle(new TaskAttemptDiagnosticsUpdateEvent(attemptID,
            diagnostics));
      }      
    }
    return newContainers;
  }

  private void updateAMRMToken(Token token) throws IOException {
    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> amrmToken =
        new org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>(token
          .getIdentifier().array(), token.getPassword().array(), new Text(
          token.getKind()), new Text(token.getService()));
    UserGroupInformation currentUGI = UserGroupInformation.getCurrentUser();
    if (UserGroupInformation.isSecurityEnabled()) {
      currentUGI = UserGroupInformation.getLoginUser();
    }
    currentUGI.addToken(amrmToken);
  }

  /**
   * 当容器完成后.创建一个事件
   * @param cont 最终容器完成时候的状态
   * @param attemptID 尝试任务ID
   */
  @VisibleForTesting
  public TaskAttemptEvent createContainerFinishedEvent(ContainerStatus cont,
      TaskAttemptId attemptID) {
    if (cont.getExitStatus() == ContainerExitStatus.ABORTED
        || cont.getExitStatus() == ContainerExitStatus.PREEMPTED) {
      // killed by framework
      return new TaskAttemptEvent(attemptID,
          TaskAttemptEventType.TA_KILL);
    } else {
      return new TaskAttemptEvent(attemptID,
          TaskAttemptEventType.TA_CONTAINER_COMPLETED);
    }
  }
  
  /**
   * 心跳过后,返回节点信息,该方法主要更新节点信息
   */
  @SuppressWarnings("unchecked")
  private void handleUpdatedNodes(AllocateResponse response) {
    // send event to the job about on updated nodes
    List<NodeReport> updatedNodes = response.getUpdatedNodes();
    if (!updatedNodes.isEmpty()) {

      // send event to the job to act upon completed tasks更新节点信息事件
      eventHandler.handle(new JobUpdatedNodesEvent(getJob().getID(),
          updatedNodes));

      // act upon running tasks 添加不可用的节点
      HashSet<NodeId> unusableNodes = new HashSet<NodeId>();
      for (NodeReport nr : updatedNodes) {
        NodeState nodeState = nr.getNodeState();
        if (nodeState.isUnusable()) {
          unusableNodes.add(nr.getNodeId());
        }
      }
      for (int i = 0; i < 2; ++i) {//分别遍历map和reduce集合,将在失效节点上的任务kill掉
        HashMap<TaskAttemptId, Container> taskSet = i == 0 ? assignedRequests.maps
            : assignedRequests.reduces;
        // kill running containers
        for (Map.Entry<TaskAttemptId, Container> entry : taskSet.entrySet()) {
          TaskAttemptId tid = entry.getKey();
          NodeId taskAttemptNodeId = entry.getValue().getNodeId();
          if (unusableNodes.contains(taskAttemptNodeId)) {
            LOG.info("Killing taskAttempt:" + tid
                + " because it is running on unusable node:"
                + taskAttemptNodeId);
            eventHandler.handle(new TaskAttemptKillEvent(tid,
                "TaskAttempt killed because it ran on unusable node"
                    + taskAttemptNodeId));
          }
        }
      }
    }
  }

  @Private
  public Resource getResourceLimit() {
    Resource headRoom = getAvailableResources();
    if (headRoom == null) {
      headRoom = Resources.none();
    }
    Resource assignedMapResource =
        Resources.multiply(mapResourceRequest, assignedRequests.maps.size());
    Resource assignedReduceResource =
        Resources.multiply(reduceResourceRequest,
          assignedRequests.reduces.size());
    return Resources.add(headRoom,
      Resources.add(assignedMapResource, assignedReduceResource));
  }

  @Private
  @VisibleForTesting
  class ScheduledRequests {
    
    //优先要被请求的尝试任务
    private final LinkedList<TaskAttemptId> earlierFailedMaps = new LinkedList<TaskAttemptId>(); 
    
    /** Maps from a host to a list of Map tasks with data on the host 
     * 该host上对应运行的尝试任务集合
     * 该rack上对应运行的尝试任务集合
     * */
    private final Map<String, LinkedList<TaskAttemptId>> mapsHostMapping = new HashMap<String, LinkedList<TaskAttemptId>>(); 
    private final Map<String, LinkedList<TaskAttemptId>> mapsRackMapping = new HashMap<String, LinkedList<TaskAttemptId>>(); 
      
    //存放每一个Map尝试任务,对应的请求资源
    @VisibleForTesting
    final Map<TaskAttemptId, ContainerRequest> maps = new LinkedHashMap<TaskAttemptId, ContainerRequest>();
      
    //存放每一个Reduce尝试任务,对应的请求资源
    private final LinkedHashMap<TaskAttemptId, ContainerRequest> reduces = new LinkedHashMap<TaskAttemptId, ContainerRequest>(); 
    
    /**
     * 移除一个尝试任务
     */
    boolean remove(TaskAttemptId tId) {
      ContainerRequest req = null;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        req = maps.remove(tId);
      } else {
        req = reduces.remove(tId);
      }
      
      if (req == null) {
        return false;
      } else {
        decContainerReq(req);
        return true;
      }
    }
    
    /**
     * 移除一个reduce的尝试任务
     */
    ContainerRequest removeReduce() {
      Iterator<Entry<TaskAttemptId, ContainerRequest>> it = reduces.entrySet().iterator();
      if (it.hasNext()) {
        Entry<TaskAttemptId, ContainerRequest> entry = it.next();
        it.remove();
        decContainerReq(entry.getValue());
        return entry.getValue();
      }
      return null;
    }
    
    /**
     * 添加一个Map资源容器请求
     */
    void addMap(ContainerRequestEvent event) {
      ContainerRequest request = null;
      
      if (event.getEarlierAttemptFailed()) {
        earlierFailedMaps.add(event.getAttemptID());
        //创建资源请求对象
        request = new ContainerRequest(event, PRIORITY_FAST_FAIL_MAP);
        LOG.info("Added "+event.getAttemptID()+" to list of failed maps");
      } else {
        //设置host和rack上运行的尝试任务集合
        for (String host : event.getHosts()) {
          LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
          if (list == null) {
            list = new LinkedList<TaskAttemptId>();
            mapsHostMapping.put(host, list);
          }
          list.add(event.getAttemptID());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Added attempt req to host " + host);
          }
       }
       for (String rack: event.getRacks()) {
         LinkedList<TaskAttemptId> list = mapsRackMapping.get(rack);
         if (list == null) {
           list = new LinkedList<TaskAttemptId>();
           mapsRackMapping.put(rack, list);
         }
         list.add(event.getAttemptID());
         if (LOG.isDebugEnabled()) {
            LOG.debug("Added attempt req to rack " + rack);
         }
       }
       //根据Map优先级生成请求资源对象
       request = new ContainerRequest(event, PRIORITY_MAP);
      }
      //添加该map资源任务请求
      maps.put(event.getAttemptID(), request);
      addContainerReq(request);
    }
    
    /**
     * 添加一个Reduce资源容器请求
     */
    void addReduce(ContainerRequest req) {
      reduces.put(req.attemptID, req);
      addContainerReq(req);
    }
    
    // this method will change the list of allocatedContainers.
    private void assign(List<Container> allocatedContainers) { 
      Iterator<Container> it = allocatedContainers.iterator();
      LOG.info("Got allocated containers " + allocatedContainers.size());
      containersAllocated += allocatedContainers.size();
      while (it.hasNext()) {
        Container allocated = it.next();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigning container " + allocated.getId()
              + " with priority " + allocated.getPriority() + " to NM "
              + allocated.getNodeId());
        }
        
        // check if allocated container meets memory requirements 
        // and whether we have any scheduled tasks that need 
        // a container to be assigned
        boolean isAssignable = true;//true表示容器是否可以被分配出去
        Priority priority = allocated.getPriority();
        Resource allocatedResource = allocated.getResource();
        if (PRIORITY_FAST_FAIL_MAP.equals(priority) 
            || PRIORITY_MAP.equals(priority)) {
          //检查分配容器的资源allocatedResource是否能满足map的资源mapResourceRequest,能满足几个
          if (ResourceCalculatorUtils.computeAvailableContainers(allocatedResource,
              mapResourceRequest, getSchedulerResourceTypes()) <= 0
              || maps.isEmpty()) {//说明不能满足
            LOG.info("Cannot assign container " + allocated 
                + " for a map as either "
                + " container memory less than required " + mapResourceRequest
                + " or no pending map tasks - maps.isEmpty=" 
                + maps.isEmpty()); 
            isAssignable = false; 
          }
        } 
        else if (PRIORITY_REDUCE.equals(priority)) {
          if (ResourceCalculatorUtils.computeAvailableContainers(allocatedResource,
              reduceResourceRequest, getSchedulerResourceTypes()) <= 0
              || reduces.isEmpty()) {//说明不能满足reduce所需要的资源
            LOG.info("Cannot assign container " + allocated 
                + " for a reduce as either "
                + " container memory less than required " + reduceResourceRequest
                + " or no pending reduce tasks - reduces.isEmpty=" 
                + reduces.isEmpty()); 
            isAssignable = false;
          }
        } else {
          LOG.warn("Container allocated at unwanted priority: " + priority + 
              ". Returning to RM...");
          isAssignable = false;
        }
        
        if(!isAssignable) {//不可用的容器资源
          // release container if we could not assign it 释放该容器
          containerNotAssigned(allocated);
          it.remove();
          continue;
        }
        
        // do not assign if allocated container is on a  如果容器在黑名单上,则也不进行分配
        // blacklisted host 分配的节点host
        String allocatedHost = allocated.getNodeId().getHost();
        if (isNodeBlacklisted(allocatedHost)) {//判断该节点是否是黑名单
          // we need to request for a new container 
          // and release the current one因为是在黑名单中,因此我们需要申请一个新的容器,并且释放掉当前黑名单的容器
          LOG.info("Got allocated container on a blacklisted "
              + " host "+allocatedHost
              +". Releasing container " + allocated);

          // find the request matching this allocated container 
          // and replace it with a new one  重新获取一个容器,新容器要代替参数容器,因为参数容器已经是黑名单等容器,因此不需要该容器,需要重新换一个容器
          ContainerRequest toBeReplacedReq = getContainerReqToReplace(allocated); 
          if (toBeReplacedReq != null) {
            LOG.info("Placing a new container request for task attempt " 
                + toBeReplacedReq.attemptID);
            ContainerRequest newReq = 
                getFilteredContainerRequest(toBeReplacedReq);
            decContainerReq(toBeReplacedReq);
            if (toBeReplacedReq.attemptID.getTaskId().getTaskType() ==
                TaskType.MAP) {
              maps.put(newReq.attemptID, newReq);
            }
            else {
              reduces.put(newReq.attemptID, newReq);
            }
            addContainerReq(newReq);
          }
          else {
            LOG.info("Could not map allocated container to a valid request."
                + " Releasing allocated container " + allocated);
          }
          
          // release container if we could not assign it 
          containerNotAssigned(allocated);
          it.remove();
          continue;
        }
      }

      //对容器进行分配
      assignContainers(allocatedContainers);
       
      // release container if we could not assign it 将没有分配的容器释放掉
      it = allocatedContainers.iterator();
      while (it.hasNext()) {
        Container allocated = it.next();
        LOG.info("Releasing unassigned and invalid container " 
            + allocated + ". RM may have assignment issues");
        containerNotAssigned(allocated);
      }
    }
    
    @SuppressWarnings("unchecked")
    private void containerAssigned(Container allocated, 
                                    ContainerRequest assigned) {
      // Update resource requests
      decContainerReq(assigned);

      // send the container-assigned event to task attempt
      eventHandler.handle(new TaskAttemptContainerAssignedEvent(
          assigned.attemptID, allocated, applicationACLs));

      assignedRequests.add(allocated, assigned.attemptID);

      if (LOG.isDebugEnabled()) {
        LOG.info("Assigned container (" + allocated + ") "
            + " to task " + assigned.attemptID + " on node "
            + allocated.getNodeId().toString());
      }
    }
    
    //释放该容器,因为该容器不能被分配
    private void containerNotAssigned(Container allocated) {
      containersReleased++;
      pendingRelease.add(allocated.getId());
      release(allocated.getId());      
    }
    
    private ContainerRequest assignWithoutLocality(Container allocated) {
      ContainerRequest assigned = null;
      
      Priority priority = allocated.getPriority();
      if (PRIORITY_FAST_FAIL_MAP.equals(priority)) {
        LOG.info("Assigning container " + allocated + " to fast fail map");
        assigned = assignToFailedMap(allocated);
      } else if (PRIORITY_REDUCE.equals(priority)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigning container " + allocated + " to reduce");
        }
        assigned = assignToReduce(allocated);
      }
        
      return assigned;
    }
        
    //对容器进行分配
    private void assignContainers(List<Container> allocatedContainers) {
      Iterator<Container> it = allocatedContainers.iterator();
      while (it.hasNext()) {
        Container allocated = it.next();
        ContainerRequest assigned = assignWithoutLocality(allocated);
        if (assigned != null) {
          containerAssigned(allocated, assigned);
          it.remove();
        }
      }
      assignMapsWithLocality(allocatedContainers);
    }
    
    /**
     * 重新获取一个容器,新容器要代替参数容器,因为参数容器已经是黑名单等容器,因此不需要该容器,需要重新换一个容器
     */
    private ContainerRequest getContainerReqToReplace(Container allocated) {
      LOG.info("Finding containerReq for allocated container: " + allocated);
      Priority priority = allocated.getPriority();
      ContainerRequest toBeReplaced = null;
      if (PRIORITY_FAST_FAIL_MAP.equals(priority)) {
        LOG.info("Replacing FAST_FAIL_MAP container " + allocated.getId());
        Iterator<TaskAttemptId> iter = earlierFailedMaps.iterator();
        while (toBeReplaced == null && iter.hasNext()) {
          toBeReplaced = maps.get(iter.next());
        }
        LOG.info("Found replacement: " + toBeReplaced);
        return toBeReplaced;
      }
      else if (PRIORITY_MAP.equals(priority)) {
        LOG.info("Replacing MAP container " + allocated.getId());
        // allocated container was for a map
        String host = allocated.getNodeId().getHost();
        LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
        if (list != null && list.size() > 0) {
          TaskAttemptId tId = list.removeLast();
          if (maps.containsKey(tId)) {
            toBeReplaced = maps.remove(tId);
          }
        }
        else {
          TaskAttemptId tId = maps.keySet().iterator().next();
          toBeReplaced = maps.remove(tId);          
        }
      }
      else if (PRIORITY_REDUCE.equals(priority)) {
        TaskAttemptId tId = reduces.keySet().iterator().next();
        toBeReplaced = reduces.remove(tId);    
      }
      LOG.info("Found replacement: " + toBeReplaced);
      return toBeReplaced;
    }
    
    /**
     * 从map中获取一个容器作为请求
     */
    @SuppressWarnings("unchecked")
    private ContainerRequest assignToFailedMap(Container allocated) {
      //try to assign to earlierFailedMaps if present
      ContainerRequest assigned = null;
      while (assigned == null && earlierFailedMaps.size() > 0) {
        TaskAttemptId tId = earlierFailedMaps.removeFirst();      
        if (maps.containsKey(tId)) {
          assigned = maps.remove(tId);
          JobCounterUpdateEvent jce = new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
          jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
          eventHandler.handle(jce);
          LOG.info("Assigned from earlierFailedMaps");
          break;
        }
      }
      return assigned;
    }
    
    /**
     * 从reduce队里中获取一个容器请求
     */
    private ContainerRequest assignToReduce(Container allocated) {
      ContainerRequest assigned = null;
      //try to assign to reduces if present
      if (assigned == null && reduces.size() > 0) {
        TaskAttemptId tId = reduces.keySet().iterator().next();
        assigned = reduces.remove(tId);
        LOG.info("Assigned to reduce");
      }
      return assigned;
    }
    
    /**
     * 从map中获取容器请求
     */
    @SuppressWarnings("unchecked")
    private void assignMapsWithLocality(List<Container> allocatedContainers) {
      // try to assign to all nodes first to match node local
      Iterator<Container> it = allocatedContainers.iterator();
      while(it.hasNext() && maps.size() > 0){
        Container allocated = it.next();        
        Priority priority = allocated.getPriority();
        assert PRIORITY_MAP.equals(priority);
        // "if (maps.containsKey(tId))" below should be almost always true.
        // hence this while loop would almost always have O(1) complexity
        String host = allocated.getNodeId().getHost();
        LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
        while (list != null && list.size() > 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Host matched to the request list " + host);
          }
          TaskAttemptId tId = list.removeFirst();
          if (maps.containsKey(tId)) {
            ContainerRequest assigned = maps.remove(tId);
            containerAssigned(allocated, assigned);
            it.remove();
            JobCounterUpdateEvent jce =
              new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
            jce.addCounterUpdate(JobCounter.DATA_LOCAL_MAPS, 1);
            eventHandler.handle(jce);
            hostLocalAssigned++;
            if (LOG.isDebugEnabled()) {
              LOG.debug("Assigned based on host match " + host);
            }
            break;
          }
        }
      }
      
      // try to match all rack local
      it = allocatedContainers.iterator();
      while(it.hasNext() && maps.size() > 0){
        Container allocated = it.next();
        Priority priority = allocated.getPriority();
        assert PRIORITY_MAP.equals(priority);
        // "if (maps.containsKey(tId))" below should be almost always true.
        // hence this while loop would almost always have O(1) complexity
        String host = allocated.getNodeId().getHost();
        String rack = RackResolver.resolve(host).getNetworkLocation();
        LinkedList<TaskAttemptId> list = mapsRackMapping.get(rack);
        while (list != null && list.size() > 0) {
          TaskAttemptId tId = list.removeFirst();
          if (maps.containsKey(tId)) {
            ContainerRequest assigned = maps.remove(tId);
            containerAssigned(allocated, assigned);
            it.remove();
            JobCounterUpdateEvent jce =
              new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
            jce.addCounterUpdate(JobCounter.RACK_LOCAL_MAPS, 1);
            eventHandler.handle(jce);
            rackLocalAssigned++;
            if (LOG.isDebugEnabled()) {
              LOG.debug("Assigned based on rack match " + rack);
            }
            break;
          }
        }
      }
      
      // assign remaining
      it = allocatedContainers.iterator();
      while(it.hasNext() && maps.size() > 0){
        Container allocated = it.next();
        Priority priority = allocated.getPriority();
        assert PRIORITY_MAP.equals(priority);
        TaskAttemptId tId = maps.keySet().iterator().next();
        ContainerRequest assigned = maps.remove(tId);
        containerAssigned(allocated, assigned);
        it.remove();
        JobCounterUpdateEvent jce =
          new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
        jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
        eventHandler.handle(jce);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigned based on * match");
        }
      }
    }
  }

  @Private
  @VisibleForTesting
  class AssignedRequests {
    //将容器分配该尝试任务ID
    private final Map<ContainerId, TaskAttemptId> containerToAttemptMap = new HashMap<ContainerId, TaskAttemptId>();
    //map的尝试任务ID--容器ID
    private final LinkedHashMap<TaskAttemptId, Container> maps = new LinkedHashMap<TaskAttemptId, Container>();
    //reduce的尝试任务ID--容器ID
    @VisibleForTesting
    final LinkedHashMap<TaskAttemptId, Container> reduces = new LinkedHashMap<TaskAttemptId, Container>();
    //被抢占的reduce资源,正在等候被抢占中
    @VisibleForTesting
    final Set<TaskAttemptId> preemptionWaitingReduces = new HashSet<TaskAttemptId>();
    
    /**
     * 将容器分配该尝试任务ID
     */
    void add(Container container, TaskAttemptId tId) {
      LOG.info("Assigned container " + container.getId().toString() + " to " + tId);
      containerToAttemptMap.put(container.getId(), tId);
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        maps.put(tId, container);
      } else {
        reduces.put(tId, container);
      }
    }

    //抢占toPreempt个reduce资源,reduce任务资源被抢占,给map尝试任务使用
    @SuppressWarnings("unchecked")
    void preemptReduce(int toPreempt) {
      
      //根据任务的执行进度,对reduce任务进行排序
      List<TaskAttemptId> reduceList = new ArrayList<TaskAttemptId>(reduces.keySet());
      //sort reduces on progress
      Collections.sort(reduceList,
          new Comparator<TaskAttemptId>() {
        @Override
        public int compare(TaskAttemptId o1, TaskAttemptId o2) {
          return Float.compare(
              getJob().getTask(o1.getTaskId()).getAttempt(o1).getProgress(),
              getJob().getTask(o2.getTaskId()).getAttempt(o2).getProgress());
        }
      });
      
      for (int i = 0; i < toPreempt && reduceList.size() > 0; i++) {
        TaskAttemptId id = reduceList.remove(0);//remove the one on top
        LOG.info("Preempting " + id);
        preemptionWaitingReduces.add(id);
        eventHandler.handle(new TaskAttemptKillEvent(id, RAMPDOWN_DIAGNOSTIC));
      }
    }
    
    /**
     * 移除一个尝试任务在内存中的关系
     */
    boolean remove(TaskAttemptId tId) {
      ContainerId containerId = null;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        containerId = maps.remove(tId).getId();
      } else {
        containerId = reduces.remove(tId).getId();
        if (containerId != null) {
          boolean preempted = preemptionWaitingReduces.remove(tId);
          if (preempted) {
            LOG.info("Reduce preemption successful " + tId);
          }
        }
      }
      
      if (containerId != null) {
        containerToAttemptMap.remove(containerId);
        return true;
      }
      return false;
    }
    
    //通过容器ID获取对应的尝试任务对象
    TaskAttemptId get(ContainerId cId) {
      return containerToAttemptMap.get(cId);
    }

    //根据尝试任务ID,获取对应的容器
    ContainerId get(TaskAttemptId tId) {
      Container taskContainer;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        taskContainer = maps.get(tId);
      } else {
        taskContainer = reduces.get(tId);
      }

      if (taskContainer == null) {
        return null;
      } else {
        return taskContainer.getId();
      }
    }
  }

  private class ScheduleStats {
    int numPendingReduces;
    int numScheduledMaps;
    int numScheduledReduces;
    int numAssignedMaps;
    int numAssignedReduces;
    int numCompletedMaps;
    int numCompletedReduces;
    int numContainersAllocated;
    int numContainersReleased;

    public void updateAndLogIfChanged(String msgPrefix) {
      boolean changed = false;

      // synchronized to fix findbug warnings
      synchronized (RMContainerAllocator.this) {
        changed |= (numPendingReduces != pendingReduces.size());
        numPendingReduces = pendingReduces.size();
        changed |= (numScheduledMaps != scheduledRequests.maps.size());
        numScheduledMaps = scheduledRequests.maps.size();
        changed |= (numScheduledReduces != scheduledRequests.reduces.size());
        numScheduledReduces = scheduledRequests.reduces.size();
        changed |= (numAssignedMaps != assignedRequests.maps.size());
        numAssignedMaps = assignedRequests.maps.size();
        changed |= (numAssignedReduces != assignedRequests.reduces.size());
        numAssignedReduces = assignedRequests.reduces.size();
        changed |= (numCompletedMaps != getJob().getCompletedMaps());
        numCompletedMaps = getJob().getCompletedMaps();
        changed |= (numCompletedReduces != getJob().getCompletedReduces());
        numCompletedReduces = getJob().getCompletedReduces();
        changed |= (numContainersAllocated != containersAllocated);
        numContainersAllocated = containersAllocated;
        changed |= (numContainersReleased != containersReleased);
        numContainersReleased = containersReleased;
      }

      if (changed) {
        log(msgPrefix);
      }
    }

    public void log(String msgPrefix) {
        LOG.info(msgPrefix + "PendingReds:" + numPendingReduces +
        " ScheduledMaps:" + numScheduledMaps +
        " ScheduledReds:" + numScheduledReduces +
        " AssignedMaps:" + numAssignedMaps +
        " AssignedReds:" + numAssignedReduces +
        " CompletedMaps:" + numCompletedMaps +
        " CompletedReds:" + numCompletedReduces +
        " ContAlloc:" + numContainersAllocated +
        " ContRel:" + numContainersReleased +
        " HostLocal:" + hostLocalAssigned +
        " RackLocal:" + rackLocalAssigned);
    }
  }
}
