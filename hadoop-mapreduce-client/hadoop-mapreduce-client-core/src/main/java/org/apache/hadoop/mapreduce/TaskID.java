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

package org.apache.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableUtils;


/**
 * TaskID represents the immutable and unique identifier for 
 * a Map or Reduce Task. Each TaskID encompasses multiple attempts made to
 * execute the Map or Reduce Task, each of which are uniquely indentified by
 * their TaskAttemptID.
 * 
 * TaskID consists of 3 parts. First part is the {@link JobID}, that this 
 * TaskInProgress belongs to. Second part of the TaskID is either 'm' or 'r' 
 * representing whether the task is a map task or a reduce task. 
 * And the third part is the task number. <br> 
 * 格式:
 * 第一部分是jobId
 * 第二部分是任务类型,m或者r
 * 第三部分是该job中该task的编号,用6位数字表示
 * An example TaskID is : 
 * <code>task_200707121733_0003_m_000005</code> , which represents the
 * fifth map task in the third job running at the jobtracker 
 * started at <code>200707121733</code>. 
 * <p>
 * Applications should never construct or parse TaskID strings
 * , but rather use appropriate constructors or {@link #forName(String)} 
 * method. 
 * 
 * @see JobID
 * @see TaskAttemptID
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TaskID extends org.apache.hadoop.mapred.ID {
  protected static final String TASK = "task";
  protected static final NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(6);
  }
  
  private JobID jobId;
  private TaskType type;
  
  /**
   * Constructs a TaskID object from given {@link JobID}.  
   * @param jobId JobID that this tip belongs to 
   * @param type the {@link TaskType} of the task 任务类型是m或者r
   * @param id the tip number 就是该task在job中的编号
   */
  public TaskID(JobID jobId, TaskType type, int id) {
    super(id);
    if(jobId == null) {
      throw new IllegalArgumentException("jobId cannot be null");
    }
    this.jobId = jobId;
    this.type = type;
  }
  
  /**
   * Constructs a TaskInProgressId object from given parts.
   * @param jtIdentifier jobTracker identifier
   * @param jobId job number 
   * @param type the TaskType 
   * @param id the tip number
   */
  public TaskID(String jtIdentifier, int jobId, TaskType type, int id) {
    this(new JobID(jtIdentifier, jobId), type, id);
  }

  /**
   * Constructs a TaskID object from given {@link JobID}.
   * @param jobId JobID that this tip belongs to
   * @param isMap whether the tip is a map
   * @param id the tip number
   */
  @Deprecated
  public TaskID(JobID jobId, boolean isMap, int id) {
    this(jobId, isMap ? TaskType.MAP : TaskType.REDUCE, id);
  }

  /**
   * Constructs a TaskInProgressId object from given parts.
   * @param jtIdentifier jobTracker identifier
   * @param jobId job number
   * @param isMap whether the tip is a map
   * @param id the tip number
   */
  @Deprecated
  public TaskID(String jtIdentifier, int jobId, boolean isMap, int id) {
    this(new JobID(jtIdentifier, jobId), isMap, id);
  }
  
  public TaskID() { 
    jobId = new JobID();
  }
  
  /** Returns the {@link JobID} object that this tip belongs to */
  public JobID getJobID() {
    return jobId;
  }
  
  /**Returns whether this TaskID is a map ID */
  @Deprecated
  public boolean isMap() {
    return type == TaskType.MAP;
  }
    
  /**
   * Get the type of the task
   */
  public TaskType getTaskType() {
    return type;
  }
  
  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    TaskID that = (TaskID)o;
    return this.type == that.type && this.jobId.equals(that.jobId);
  }

  /**Compare TaskInProgressIds by first jobIds, then by tip numbers. Reduces are 
   * defined as greater then maps
   * 先比较job,然后在比较类型,最后在前面两者都相同的情况下,比较任务编号
   * */
  @Override
  public int compareTo(ID o) {
    TaskID that = (TaskID)o;
    int jobComp = this.jobId.compareTo(that.jobId);
    if(jobComp == 0) {
      if(this.type == that.type) {
        return this.id - that.id;
      }
      else {
        return this.type.compareTo(that.type);
      }
    }
    else return jobComp;
  }
  @Override
  public String toString() { 
    return appendTo(new StringBuilder(TASK)).toString();
  }

  /**
   * Add the unique string to the given builder.
   * @param builder the builder to append to
   * @return the builder that was passed in
   */
  protected StringBuilder appendTo(StringBuilder builder) {
    return jobId.appendTo(builder).
                 append(SEPARATOR).
                 append(CharTaskTypeMaps.getRepresentingCharacter(type)).
                 append(SEPARATOR).
                 append(idFormat.format(id));
  }
  
  @Override
  public int hashCode() {
    return jobId.hashCode() * 524287 + id;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);//任务id
    jobId.readFields(in);//jobId
    type = WritableUtils.readEnum(in, TaskType.class);//任务类型
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    jobId.write(out);
    WritableUtils.writeEnum(out, type);
  }
  
  /** Construct a TaskID object from given string 
   * @return constructed TaskID object or null if the given String is null
   * @throws IllegalArgumentException if the given string is malformed
   * @ str task_200707121733_0003_m_000005 解析这个串,转换成TaskID对象,相当于反序列化的过程
   */
  public static TaskID forName(String str) 
    throws IllegalArgumentException {
    if(str == null)
      return null;
    String exceptionMsg = null;
    try {
      String[] parts = str.split("_");
      if(parts.length == 5) {
        if(parts[0].equals(TASK)) {//第一个一定是task字符串
          String type = parts[3];//类型是m还是r
          TaskType t = CharTaskTypeMaps.getTaskType(type.charAt(0));//转化成对象
          if(t != null) {
          
            return new org.apache.hadoop.mapred.TaskID(parts[1], 
                                                     Integer.parseInt(parts[2]),
                                                     t, 
                                                     Integer.parseInt(parts[4]));
          } else
            exceptionMsg = "Bad TaskType identifier. TaskId string : " + str
                + " is not properly formed.";
        }
      }
    }catch (Exception ex) {//fall below
    }
    
    //解析异常
    if (exceptionMsg == null) {
      exceptionMsg = "TaskId string : " + str + " is not properly formed";
    }
    throw new IllegalArgumentException(exceptionMsg);
  }
  /**
   * Gets the character representing the {@link TaskType}
   * @param type the TaskType
   * @return the character
   */
  public static char getRepresentingCharacter(TaskType type) {
    return CharTaskTypeMaps.getRepresentingCharacter(type);
  }
  /**
   * Gets the {@link TaskType} corresponding to the character
   * @param c the character
   * @return the TaskType
   */
  public static TaskType getTaskType(char c) {
    return CharTaskTypeMaps.getTaskType(c);
  }
  
  public static String getAllTaskTypes() {
    return CharTaskTypeMaps.allTaskTypes;
  }

  /**
   * Maintains the mapping from the character representation of a task type to 
   * the enum class TaskType constants
   */
  static class CharTaskTypeMaps {
    private static EnumMap<TaskType, Character> typeToCharMap = 
      new EnumMap<TaskType,Character>(TaskType.class);
    private static Map<Character, TaskType> charToTypeMap = 
      new HashMap<Character, TaskType>();
    static String allTaskTypes = "(m|r|s|c|t)";
    static {//内存初始化映射关系
      setupTaskTypeToCharMapping();
      setupCharToTaskTypeMapping();
    }
    
    /**
     * 类型映射成字符
     */
    private static void setupTaskTypeToCharMapping() {
      typeToCharMap.put(TaskType.MAP, 'm');
      typeToCharMap.put(TaskType.REDUCE, 'r');
      typeToCharMap.put(TaskType.JOB_SETUP, 's');
      typeToCharMap.put(TaskType.JOB_CLEANUP, 'c');
      typeToCharMap.put(TaskType.TASK_CLEANUP, 't');
    }
/**
 * 字符映射成类型对象
 */
    private static void setupCharToTaskTypeMapping() {
      charToTypeMap.put('m', TaskType.MAP);
      charToTypeMap.put('r', TaskType.REDUCE);
      charToTypeMap.put('s', TaskType.JOB_SETUP);
      charToTypeMap.put('c', TaskType.JOB_CLEANUP);
      charToTypeMap.put('t', TaskType.TASK_CLEANUP);
    }

    static char getRepresentingCharacter(TaskType type) {
      return typeToCharMap.get(type);
    }
    static TaskType getTaskType(char c) {
      return charToTypeMap.get(c);
    }
  }

}
