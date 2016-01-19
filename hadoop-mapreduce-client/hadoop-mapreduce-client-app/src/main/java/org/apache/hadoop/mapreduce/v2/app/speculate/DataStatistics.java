/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
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

/**
 * 数据计算
 */
public class DataStatistics {
  private int count = 0;//次数,每次累加1
  private double sum = 0;//和
  private double sumSquares = 0;//平方

  public DataStatistics() {
  }

  public DataStatistics(double initNum) {
    this.count = 1;
    this.sum = initNum;
    this.sumSquares = initNum * initNum;
  }

  public synchronized void add(double newNum) {
    this.count++;
    this.sum += newNum;
    this.sumSquares += newNum * newNum;
  }

  public synchronized void updateStatistics(double old, double update) {
	this.sum += update - old;
	this.sumSquares += (update * update) - (old * old);
  }

  /**
   * 均值,用sum/count,即每一次的平均值是多少
   */
  public synchronized double mean() {
    return count == 0 ? 0.0 : sum/count;
  }

  /**
   * 方差
   * 平方后/count,表示平方后的均值(sumSquares/count)
   * 均值的平方mean* mean
   * 两者之差
   */
  public synchronized double var() {
    // E(X^2) - E(X)^2
    if (count <= 1) {
      return 0.0;
    }
    double mean = mean();
    return Math.max((sumSquares/count) - mean * mean, 0.0d);
  }

  /**
   * 标准差,即方差的开方
   */
  public synchronized double std() {
    return Math.sqrt(this.var());
  }

  /**
   * @param sigma 西格玛 ∑，σ 表示求和或者标准差
   * return 均值+标准差*参数传递的标准差
   */
  public synchronized double outlier(float sigma) {
    if (count != 0.0) {
      return mean() + std() * sigma;
    }

    return 0.0;
  }

  public synchronized double count() {
    return count;
  }

  public String toString() {
    return "DataStatistics: count is " + count + ", sum is " + sum +
    ", sumSquares is " + sumSquares + " mean is " + mean() + " std() is " + std();
  }
}
