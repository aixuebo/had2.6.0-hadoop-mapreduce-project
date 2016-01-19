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

import java.util.EnumSet;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;

public class ResourceCalculatorUtils {
  //计算a/b的值,向上取整
  public static int divideAndCeil(int a, int b) {
    if (b == 0) {
      return 0;
    }
    return (a + (b - 1)) / b;
  }

  /**
   * 如果包含CPU资源,因此按照cpu资源能满足多少个请求资源,内存资源能满足多少个请求资源,取最小值
   * 否则仅仅判断内存资源能满足多少个请求即可
   * @param available 可用的资源
   * @param required 请求的资源
   * @param resourceTypes 资源类型
   * return 计算可用资源可以满足多少个请求资源需求,即可以有多少个container
   */
  public static int computeAvailableContainers(Resource available,
      Resource required, EnumSet<SchedulerResourceTypes> resourceTypes) {
    if (resourceTypes.contains(SchedulerResourceTypes.CPU)) {//因为包含CPU资源,因此按照cpu资源能满足多少个请求资源,内存资源能满足多少个请求资源,取最小值
      return Math.min(available.getMemory() / required.getMemory(),
        available.getVirtualCores() / required.getVirtualCores());
    }
    return available.getMemory() / required.getMemory();
  }

  /**
   * 同computeAvailableContainers方法一样,只是返回的结果是向上取整
   */
  public static int divideAndCeilContainers(Resource required, Resource factor,
      EnumSet<SchedulerResourceTypes> resourceTypes) {
    if (resourceTypes.contains(SchedulerResourceTypes.CPU)) {
      return Math.max(divideAndCeil(required.getMemory(), factor.getMemory()),
        divideAndCeil(required.getVirtualCores(), factor.getVirtualCores()));
    }
    return divideAndCeil(required.getMemory(), factor.getMemory());
  }
}
