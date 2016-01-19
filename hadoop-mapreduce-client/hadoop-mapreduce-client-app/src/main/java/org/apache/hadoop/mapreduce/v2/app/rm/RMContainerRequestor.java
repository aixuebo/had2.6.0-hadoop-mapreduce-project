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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

import com.google.common.annotations.VisibleForTesting;


/**
 * Keeps the data structures to send container requests to RM.
 */
public abstract class RMContainerRequestor extends RMCommunicator {
  
  private static final Log LOG = LogFactory.getLog(RMContainerRequestor.class);

  protected int lastResponseID;//最后一次请求后返回的ID
  private Resource availableResources;//每次请求后,返回值中报告的集群的可用资源集合

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  //Key -> Priority
  //Value -> Map
  //Key->ResourceName (e.g., hostname, rackname, *)
  //Value->Map
  //Key->Resource Capability
  //Value->ResourceRequest
  //每个优先级--hostname/rackname/* -- 请求的所占资源  --- 具体的请求
  private final Map<Priority, Map<String, Map<Resource, ResourceRequest>>>
  remoteRequestsTable =
      new TreeMap<Priority, Map<String, Map<Resource, ResourceRequest>>>();

  // use custom comparator to make sure ResourceRequest objects differing only in 
  // numContainers dont end up as duplicates 最终等待请求的集合,按照一定规则排序了, 注意:因为每次ResourceRequest对象中的numContainers会变化,因此需要先删除,再添加
  private final Set<ResourceRequest> ask = new TreeSet<ResourceRequest>(
      new org.apache.hadoop.yarn.api.records.ResourceRequest.ResourceRequestComparator());
  //该请求内完成的容器集合
  private final Set<ContainerId> release = new TreeSet<ContainerId>();
  // pendingRelease holds history or release requests.request is removed only if
  // RM sends completedContainer.
  // How it different from release? --> release is for per allocate() request.
  //等待释放的容器集合,即已经无用的容器集合
  protected Set<ContainerId> pendingRelease = new TreeSet<ContainerId>();
  private boolean nodeBlacklistingEnabled;//是否启用黑名单过滤host功能
  private int blacklistDisablePercent;//黑名单host机器集合占总集群机器占比
  //是否忽略黑名单校验
  private AtomicBoolean ignoreBlacklisting = new AtomicBoolean(false);
  private int blacklistedNodeCount = 0;
  private int lastClusterNmCount = 0;//上一次集群数量
  private int clusterNmCount = 0;//集群数量
  private int maxTaskFailuresPerNode;//每一个host最大的失败次数.超过该次数则就是黑名单了
  
  //每一个host对应的失败次数
  private final Map<String, Integer> nodeFailures = new HashMap<String, Integer>();
  //黑名单host集合
  private final Set<String> blacklistedNodes = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  //等待要继续添加到黑名单的host集合
  private final Set<String> blacklistAdditions = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  //等待要移除黑名单的host集合
  private final Set<String> blacklistRemovals = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  public RMContainerRequestor(ClientService clientService, AppContext context) {
    super(clientService, context);
  }

  /**
   * 被请求的容器,请求对象
   */
  @Private
  @VisibleForTesting
  static class ContainerRequest {
    final TaskAttemptId attemptID;//尝试任务ID,该尝试任务使用了该容器
    final Resource capability;//该容器需要的资源
    final String[] hosts;
    final String[] racks;
    //final boolean earlierAttemptFailed;
    final Priority priority;//该容器的优先级
    /**
     * the time when this request object was formed; can be used to avoid
     * aggressive preemption for recently placed requests
     * 请求的时间,该时间可以避免攻击性的抢先占有
     */
    final long requestTimeMs;

    public ContainerRequest(ContainerRequestEvent event, Priority priority) {
      this(event.getAttemptID(), event.getCapability(), event.getHosts(),
          event.getRacks(), priority);
    }

    public ContainerRequest(ContainerRequestEvent event, Priority priority,
                            long requestTimeMs) {
      this(event.getAttemptID(), event.getCapability(), event.getHosts(),
          event.getRacks(), priority, requestTimeMs);
    }

    public ContainerRequest(TaskAttemptId attemptID,
                            Resource capability, String[] hosts, String[] racks,
                            Priority priority) {
      this(attemptID, capability, hosts, racks, priority,
          System.currentTimeMillis());
    }

    public ContainerRequest(TaskAttemptId attemptID,
        Resource capability, String[] hosts, String[] racks,
        Priority priority, long requestTimeMs) {
      this.attemptID = attemptID;
      this.capability = capability;
      this.hosts = hosts;
      this.racks = racks;
      this.priority = priority;
      this.requestTimeMs = requestTimeMs;
    }
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("AttemptId[").append(attemptID).append("]");
      sb.append("Capability[").append(capability).append("]");
      sb.append("Priority[").append(priority).append("]");
      return sb.toString();
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    nodeBlacklistingEnabled = 
      conf.getBoolean(MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
    LOG.info("nodeBlacklistingEnabled:" + nodeBlacklistingEnabled);
    maxTaskFailuresPerNode = 
      conf.getInt(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 3);
    blacklistDisablePercent =
        conf.getInt(
            MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT,
            MRJobConfig.DEFAULT_MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERCENT);
    LOG.info("maxTaskFailuresPerNode is " + maxTaskFailuresPerNode);
    if (blacklistDisablePercent < -1 || blacklistDisablePercent > 100) {
      throw new YarnRuntimeException("Invalid blacklistDisablePercent: "
          + blacklistDisablePercent
          + ". Should be an integer between 0 and 100 or -1 to disabled");
    }
    LOG.info("blacklistDisablePercent is " + blacklistDisablePercent);
  }

  //真正的发送请求和接受请求,返回是请求的返回值
  protected AllocateResponse makeRemoteRequest() throws YarnException,
      IOException {
   /**
    * 该对象存储:
   添加准备移除的黑名单(以前该host是黑名单,现在不是了,要被移除)
 * 添加新的黑名单
     */
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(new ArrayList<String>(blacklistAdditions),
            new ArrayList<String>(blacklistRemovals));
    
    
    //向远程机器发送分配资源信息,将等待分配的信息上传到远程服务器做处理
    AllocateRequest allocateRequest =
        AllocateRequest.newInstance(lastResponseID,
          super.getApplicationProgress(), new ArrayList<ResourceRequest>(ask),
          new ArrayList<ContainerId>(release), blacklistRequest);
    
    //远程服务器对这次分配的新信息进行回复
    AllocateResponse allocateResponse = scheduler.allocate(allocateRequest);
    
    lastResponseID = allocateResponse.getResponseId();
    availableResources = allocateResponse.getAvailableResources();
    lastClusterNmCount = clusterNmCount;
    clusterNmCount = allocateResponse.getNumClusterNodes();

    if (ask.size() > 0 || release.size() > 0) {
      LOG.info("getResources() for " + applicationId + ":" + " ask="
          + ask.size() + " release= " + release.size() + " newContainers="
          + allocateResponse.getAllocatedContainers().size()
          + " finishedContainers="
          + allocateResponse.getCompletedContainersStatuses().size()
          + " resourcelimit=" + availableResources + " knownNMs="
          + clusterNmCount);
    }

    ask.clear();
    release.clear();

    if (blacklistAdditions.size() > 0 || blacklistRemovals.size() > 0) {
      LOG.info("Update the blacklist for " + applicationId +
          ": blacklistAdditions=" + blacklistAdditions.size() +
          " blacklistRemovals=" +  blacklistRemovals.size());
    }
    blacklistAdditions.clear();
    blacklistRemovals.clear();
    return allocateResponse;
  }

  protected void addOutstandingRequestOnResync() {
    //将remoteRequestsTable中信息添加到ask集合中
    for (Map<String, Map<Resource, ResourceRequest>> rr : remoteRequestsTable
        .values()) {
      for (Map<Resource, ResourceRequest> capabalities : rr.values()) {
        for (ResourceRequest request : capabalities.values()) {
          addResourceRequestToAsk(request);
        }
      }
    }
    if (!ignoreBlacklisting.get()) {
      blacklistAdditions.addAll(blacklistedNodes);
    }
    if (!pendingRelease.isEmpty()) {
      release.addAll(pendingRelease);
    }
  }

  // May be incorrect if there's multiple NodeManagers running on a single host.
  // knownNodeCount is based on node managers, not hosts. blacklisting is
  // currently based on hosts.
  protected void computeIgnoreBlacklisting() {
    if (!nodeBlacklistingEnabled) {//如果黑名单不可用,则停止
      return;
    }
    //(blacklistedNodeCount != blacklistedNodes.size() || clusterNmCount != lastClusterNmCount) 说明黑名单数量有变化,或者集群数量有变化
    if (blacklistDisablePercent != -1
        && (blacklistedNodeCount != blacklistedNodes.size() ||
            clusterNmCount != lastClusterNmCount)) {
      //黑名单host集合
      blacklistedNodeCount = blacklistedNodes.size();
      if (clusterNmCount == 0) {
        LOG.info("KnownNode Count at 0. Not computing ignoreBlacklisting");
        return;
      }
      //黑名单host数量占集群机器总数量的百分比
      int val = (int) ((float) blacklistedNodes.size() / clusterNmCount * 100);
      if (val >= blacklistDisablePercent) {//如果大于最大比例
        if (ignoreBlacklisting.compareAndSet(false, true)) {//将忽略黑名单添加,因为黑名单已经设置成最大了
          LOG.info("Ignore blacklisting set to true. Known: " + clusterNmCount
              + ", Blacklisted: " + blacklistedNodeCount + ", " + val + "%");
          // notify RM to ignore all the blacklisted nodes
          blacklistAdditions.clear();
          blacklistRemovals.addAll(blacklistedNodes);
        }
      } else {
        if (ignoreBlacklisting.compareAndSet(true, false)) {//将忽略黑名单的选项设置成false,因为不能忽略黑名单了,因为有黑名单占比没到一定比例
          LOG.info("Ignore blacklisting set to false. Known: " + clusterNmCount
              + ", Blacklisted: " + blacklistedNodeCount + ", " + val + "%");
          // notify RM of all the blacklisted nodes
          blacklistAdditions.addAll(blacklistedNodes);
          blacklistRemovals.clear();
        }
      }
    }
  }
  
  /**
   * 计算该host的失败次数
   * 接收到ContainerAllocator类的CONTAINER_FAILED事件
   */
  protected void containerFailedOnHost(String hostName) {
    if (!nodeBlacklistingEnabled) {
      return;
    }
    if (blacklistedNodes.contains(hostName)) {//如果在黑名单中,则不再计算
      if (LOG.isDebugEnabled()) {
        LOG.debug("Host " + hostName + " is already blacklisted.");
      }
      return; //already blacklisted
    }
    
    //计算该host以前的失败次数,并且累加
    Integer failures = nodeFailures.remove(hostName);
    failures = failures == null ? Integer.valueOf(0) : failures;
    failures++;
    LOG.info(failures + " failures on node " + hostName);
    if (failures >= maxTaskFailuresPerNode) {//超过节点最大的失败次数
      blacklistedNodes.add(hostName);//黑名单加入该host
      if (!ignoreBlacklisting.get()) {//添加该host
        blacklistAdditions.add(hostName);
      }
      //Even if blacklisting is ignored, continue to remove the host from
      // the request table. The RM may have additional nodes it can allocate on.
      LOG.info("Blacklisted host " + hostName);

      //remove all the requests corresponding to this hostname
      for (Map<String, Map<Resource, ResourceRequest>> remoteRequests 
          : remoteRequestsTable.values()){
        //remove from host if no pending allocations
        boolean foundAll = true;
        //循环该host下所有的资源
        Map<Resource, ResourceRequest> reqMap = remoteRequests.get(hostName);
        if (reqMap != null) {
          for (ResourceRequest req : reqMap.values()) {
            if (!ask.remove(req)) {//移除不成功
              foundAll = false;
              // if ask already sent to RM, we can try and overwrite it if possible.
              // send a new ask to RM with numContainers
              // specified for the blacklisted host to be 0.
              ResourceRequest zeroedRequest =
                  ResourceRequest.newInstance(req.getPriority(),
                    req.getResourceName(), req.getCapability(),
                    req.getNumContainers(), req.getRelaxLocality());

              zeroedRequest.setNumContainers(0);
              // to be sent to RM on next heartbeat
              addResourceRequestToAsk(zeroedRequest);
            }
          }
          // if all requests were still in ask queue
          // we can remove this request
          if (foundAll) {
            remoteRequests.remove(hostName);
          }
        }
        // TODO handling of rack blacklisting
        // Removing from rack should be dependent on no. of failures within the rack 
        // Blacklisting a rack on the basis of a single node's blacklisting 
        // may be overly aggressive. 
        // Node failures could be co-related with other failures on the same rack 
        // but we probably need a better approach at trying to decide how and when 
        // to blacklist a rack
      }
    } else {
      nodeFailures.put(hostName, failures);
    }
  }

  protected Resource getAvailableResources() {
    return availableResources;
  }
  
  //添加一个容器
  protected void addContainerReq(ContainerRequest req) {
    // Create resource requests
    for (String host : req.hosts) {
      // Data-local
      if (!isNodeBlacklisted(host)) {
        addResourceRequest(req.priority, host, req.capability);
      }
    }

    // Nothing Rack-local for now
    for (String rack : req.racks) {
      addResourceRequest(req.priority, rack, req.capability);
    }

    // Off-switch
    addResourceRequest(req.priority, ResourceRequest.ANY, req.capability);
  }

  //减少一个容器
  protected void decContainerReq(ContainerRequest req) {
    // Update resource requests
    for (String hostName : req.hosts) {
      decResourceRequest(req.priority, hostName, req.capability);
    }
    
    for (String rack : req.racks) {
      decResourceRequest(req.priority, rack, req.capability);
    }
   
    decResourceRequest(req.priority, ResourceRequest.ANY, req.capability);
  }

  /**
   * 添加一个资源请求
   * @param priority 尝试任务的执行优先级
   * @param resourceName 资源名称,即host、rack、*any通配符
   * @param capability 所需要的资源
   */
  private void addResourceRequest(Priority priority, String resourceName,
      Resource capability) {
    //该优先级对应的集合
    Map<String, Map<Resource, ResourceRequest>> remoteRequests = this.remoteRequestsTable.get(priority);
    if (remoteRequests == null) {
      remoteRequests = new HashMap<String, Map<Resource, ResourceRequest>>();
      this.remoteRequestsTable.put(priority, remoteRequests);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Added priority=" + priority);
      }
    }
    //优先级--hostname/rack/* 对应的集合
    Map<Resource, ResourceRequest> reqMap = remoteRequests.get(resourceName);
    if (reqMap == null) {
      reqMap = new HashMap<Resource, ResourceRequest>();
      remoteRequests.put(resourceName, reqMap);
    }
    
    //优先级--hostname/rack/*--所需要的内存资源+cpu资源对应的ResourceRequest对象
    ResourceRequest remoteRequest = reqMap.get(capability);
    if (remoteRequest == null) {
      remoteRequest = recordFactory.newRecordInstance(ResourceRequest.class);
      remoteRequest.setPriority(priority);
      remoteRequest.setResourceName(resourceName);
      remoteRequest.setCapability(capability);
      remoteRequest.setNumContainers(0);
      reqMap.put(capability, remoteRequest);
    }
    //累加该资源对应的容器数量
    remoteRequest.setNumContainers(remoteRequest.getNumContainers() + 1);

    // Note this down for next interaction with ResourceManager
    addResourceRequestToAsk(remoteRequest);
    if (LOG.isDebugEnabled()) {
      LOG.debug("addResourceRequest:" + " applicationId="
          + applicationId.getId() + " priority=" + priority.getPriority()
          + " resourceName=" + resourceName + " numContainers="
          + remoteRequest.getNumContainers() + " #asks=" + ask.size());
    }
  }

  /**
   * 减少一个请求
   * @param priority
   * @param resourceName
   * @param capability
   */
  private void decResourceRequest(Priority priority, String resourceName,
      Resource capability) {
    //优先级对应的集合
    Map<String, Map<Resource, ResourceRequest>> remoteRequests = this.remoteRequestsTable.get(priority);
    //优先级--hostname/rack/* 对应的集合
    Map<Resource, ResourceRequest> reqMap = remoteRequests.get(resourceName);
    if (reqMap == null) {
      // as we modify the resource requests by filtering out blacklisted hosts 
      // when they are added, this value may be null when being 
      // decremented
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not decrementing resource as " + resourceName
            + " is not present in request table");
      }
      return;
    }
    
    //优先级--hostname/rack/*--所需要的内存资源+cpu资源对应的ResourceRequest对象
    ResourceRequest remoteRequest = reqMap.get(capability);

    if (LOG.isDebugEnabled()) {
      LOG.debug("BEFORE decResourceRequest:" + " applicationId="
          + applicationId.getId() + " priority=" + priority.getPriority()
          + " resourceName=" + resourceName + " numContainers="
          + remoteRequest.getNumContainers() + " #asks=" + ask.size());
    }

    //减少该资源的一个请求容器
    if(remoteRequest.getNumContainers() > 0) {
      // based on blacklisting comments above we can end up decrementing more 
      // than requested. so guard for that.
      remoteRequest.setNumContainers(remoteRequest.getNumContainers() -1);
    }
    
    //如果没有容器了,则删除一些数据
    if (remoteRequest.getNumContainers() == 0) {
      reqMap.remove(capability);
      if (reqMap.size() == 0) {
        remoteRequests.remove(resourceName);
      }
      if (remoteRequests.size() == 0) {
        remoteRequestsTable.remove(priority);
      }
    }

    // send the updated resource request to RM
    // send 0 container count requests also to cancel previous requests
    addResourceRequestToAsk(remoteRequest);

    if (LOG.isDebugEnabled()) {
      LOG.info("AFTER decResourceRequest:" + " applicationId="
          + applicationId.getId() + " priority=" + priority.getPriority()
          + " resourceName=" + resourceName + " numContainers="
          + remoteRequest.getNumContainers() + " #asks=" + ask.size());
    }
  }
  
  //更新ask中的数据
  private void addResourceRequestToAsk(ResourceRequest remoteRequest) {
    // because objects inside the resource map can be deleted ask can end up 
    // containing an object that matches new resource object but with different
    // numContainers. So exisintg values must be replaced explicitly
    //因为每次ResourceRequest对象中的numContainers会变化,因此需要先删除,再添加
    if(ask.contains(remoteRequest)) {
      ask.remove(remoteRequest);
    }
    ask.add(remoteRequest);
  }

  protected void release(ContainerId containerId) {
    release.add(containerId);
  }
  
  //检查该host是否是在黑名单中
  protected boolean isNodeBlacklisted(String hostname) {
    if (!nodeBlacklistingEnabled || ignoreBlacklisting.get()) {
      return false;
    }
    return blacklistedNodes.contains(hostname);
  }

  /**
   * 将原始请求资源中host集合,过滤一部分黑名单的,最终获取的是过滤后的ContainerRequest对象
   */
  protected ContainerRequest getFilteredContainerRequest(ContainerRequest orig) {
    ArrayList<String> newHosts = new ArrayList<String>();
    for (String host : orig.hosts) {
      if (!isNodeBlacklisted(host)) {
        newHosts.add(host);      
      }
    }
    String[] hosts = newHosts.toArray(new String[newHosts.size()]);
    ContainerRequest newReq = new ContainerRequest(orig.attemptID, orig.capability,
        hosts, orig.racks, orig.priority); 
    return newReq;
  }
  
  /**
   * 获取黑名单host集合
   */
  public Set<String> getBlacklistedNodes() {
    return blacklistedNodes;
  }
}
