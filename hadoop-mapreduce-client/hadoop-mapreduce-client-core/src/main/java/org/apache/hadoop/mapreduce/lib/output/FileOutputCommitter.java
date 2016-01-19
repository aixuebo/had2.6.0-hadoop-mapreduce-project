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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/** An {@link OutputCommitter} that commits files specified 
 * in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}.
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileOutputCommitter extends OutputCommitter {
  private static final Log LOG = LogFactory.getLog(FileOutputCommitter.class);

  /** 
   * Name of directory where pending data is placed.  Data that has not been
   * committed yet.文件尚未完成的时候的名字,即等候的时候名称
   */
  public static final String PENDING_DIR_NAME = "_temporary";
  /**
   * Temporary directory name 
   *
   * The static variable to be compatible with M/R 1.x
   */
  @Deprecated
  protected static final String TEMP_DIR_NAME = PENDING_DIR_NAME;
  public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
  public static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER = 
    "mapreduce.fileoutputcommitter.marksuccessfuljobs";//标示是否成功完成了该job,如果该参数设置为true,则要当job完成后标示该job是完成的,即在该输出目录里面生成一个_SUCCESS的空目录即可
  private Path outputPath = null;//输出目录
  private Path workPath = null;//工作目录 return out/_temporary/appAttemptId/_temporary/TaskAttemptID

  /**
   * Create a file output committer
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a noop.
   * @param context the task's context
   * @throws IOException
   */
  public FileOutputCommitter(Path outputPath, 
                             TaskAttemptContext context) throws IOException {
    this(outputPath, (JobContext)context);
    if (outputPath != null) {
      workPath = getTaskAttemptPath(context, outputPath);
    }
  }
  
  /**
   * Create a file output committer
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a noop.
   * @param context the task's context
   * @throws IOException
   */
  @Private
  public FileOutputCommitter(Path outputPath, 
                             JobContext context) throws IOException {
    if (outputPath != null) {
      FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
      this.outputPath = fs.makeQualified(outputPath);
    }
  }
  
  /**
   * @return the path where final output of the job should be placed.  This
   * could also be considered the committed application attempt path.
   * 输出目录
   */
  private Path getOutputPath() {
    return this.outputPath;
  }
  
  /**
   * @return true if we have an output path set, else false.
   * 是否设置了输出目录
   */
  private boolean hasOutputPath() {
    return this.outputPath != null;
  }
  
  /**
   * @return the path where the output of pending job attempts are
   * stored.
   * 获取任务没完成时候的文件全路径
   * return out/_temporary
   */
  private Path getPendingJobAttemptsPath() {
    return getPendingJobAttemptsPath(getOutputPath());
  }
  
  /**
   * Get the location of pending job attempts.
   * @param out the base output directory.
   * @return the location of pending job attempts.
   * 获取任务没完成时候的文件全路径
   * return out/_temporary
   */
  private static Path getPendingJobAttemptsPath(Path out) {
    return new Path(out, PENDING_DIR_NAME);
  }
  
  /**
   * Get the Application Attempt Id for this job
   * @param context the context to look in
   * @return the Application Attempt Id for a given job.
   * 获取job的AttemptId
   */
  private static int getAppAttemptId(JobContext context) {
    return context.getConfiguration().getInt(
        MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
  }
  
  /**
   * Compute the path where the output of a given job attempt will be placed. 
   * @param context the context of the job.  This is used to get the
   * application attempt id.
   * @return the path to store job attempt data.
   * 根据获取job的AttemptId和输出目录,获取Attempt任务的临时路径
   * return out/_temporary/appAttemptId
   */
  public Path getJobAttemptPath(JobContext context) {
    return getJobAttemptPath(context, getOutputPath());
  }
  
  /**
   * Compute the path where the output of a given job attempt will be placed. 
   * @param context the context of the job.  This is used to get the
   * application attempt id.
   * @param out the output path to place these in.
   * @return the path to store job attempt data.
   * 根据获取job的AttemptId和输出目录,获取Attempt任务的临时路径
   * return out/_temporary/appAttemptId
   */
  public static Path getJobAttemptPath(JobContext context, Path out) {
    return getJobAttemptPath(getAppAttemptId(context), out);
  }
  
  /**
   * Compute the path where the output of a given job attempt will be placed. 
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   * 根据获取job的AttemptId和输出目录,获取Attempt任务的临时路径
   * return out/_temporary/appAttemptId
   */
  protected Path getJobAttemptPath(int appAttemptId) {
    return getJobAttemptPath(appAttemptId, getOutputPath());
  }
  
  /**
   * Compute the path where the output of a given job attempt will be placed. 
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   * 根据获取job的AttemptId和输出目录,获取Attempt任务的临时路径
   * return out/_temporary/appAttemptId
   */
  private static Path getJobAttemptPath(int appAttemptId, Path out) {
    return new Path(getPendingJobAttemptsPath(out), String.valueOf(appAttemptId));
  }
  
  /**
   * Compute the path where the output of pending task attempts are stored.
   * @param context the context of the job with pending tasks. 
   * @return the path where the output of pending task attempts are stored.
   * 尚未完成时task路径
   * return out/_temporary/appAttemptId/_temporary
   */
  private Path getPendingTaskAttemptsPath(JobContext context) {
    return getPendingTaskAttemptsPath(context, getOutputPath());
  }
  
  /**
   * Compute the path where the output of pending task attempts are stored.
   * @param context the context of the job with pending tasks. 
   * @return the path where the output of pending task attempts are stored.
   * 尚未完成时task路径
   * return out/_temporary/appAttemptId/_temporary
   */
  private static Path getPendingTaskAttemptsPath(JobContext context, Path out) {
    return new Path(getJobAttemptPath(context, out), PENDING_DIR_NAME);
  }
  
  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * 
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   * task的工作目录
   * return out/_temporary/appAttemptId/_temporary/TaskAttemptID
   */
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return new Path(getPendingTaskAttemptsPath(context), 
        String.valueOf(context.getTaskAttemptID()));
  }
  
  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * 
   * @param context the context of the task attempt.
   * @param out The output path to put things in.
   * @return the path where a task attempt should be stored.
   * task的工作目录
   * return out/_temporary/appAttemptId/_temporary/TaskAttemptID
   */
  public static Path getTaskAttemptPath(TaskAttemptContext context, Path out) {
    return new Path(getPendingTaskAttemptsPath(context, out), 
        String.valueOf(context.getTaskAttemptID()));
  }
  
  /**
   * Compute the path where the output of a committed task is stored until
   * the entire job is committed.
   * @param context the context of the task attempt
   * @return the path where the output of a committed task is stored until
   * the entire job is committed.
   * 
   * task的完成后的工作目录
   * return out/_temporary/appAttemptId/TaskAttemptID
   */
  public Path getCommittedTaskPath(TaskAttemptContext context) {
    return getCommittedTaskPath(getAppAttemptId(context), context);
  }
  
  /**
   * task的完成后的工作目录
   * return out/_temporary/appAttemptId/TaskAttemptID 
   */
  public static Path getCommittedTaskPath(TaskAttemptContext context, Path out) {
    return getCommittedTaskPath(getAppAttemptId(context), context, out);
  }
  
  /**
   * Compute the path where the output of a committed task is stored until the
   * entire job is committed for a specific application attempt.
   * @param appAttemptId the id of the application attempt to use
   * @param context the context of any task.
   * @return the path where the output of a committed task is stored.
   * 
   * task的完成后的工作目录
   * return out/_temporary/appAttemptId/TaskAttemptID
   */
  protected Path getCommittedTaskPath(int appAttemptId, TaskAttemptContext context) {
    return new Path(getJobAttemptPath(appAttemptId),
        String.valueOf(context.getTaskAttemptID().getTaskID()));
  }
  
  /**
   * task的完成后的工作目录
   * return out/_temporary/appAttemptId/TaskAttemptID
   */
  private static Path getCommittedTaskPath(int appAttemptId, TaskAttemptContext context, Path out) {
    return new Path(getJobAttemptPath(appAttemptId, out),
        String.valueOf(context.getTaskAttemptID().getTaskID()));
  }
  
  private static class CommittedTaskFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return !PENDING_DIR_NAME.equals(path.getName());
    }
  }
  
  /**
   * Get a list of all paths where output from committed tasks are stored.
   * @param context the context of the current job
   * @return the list of these Paths/FileStatuses. 
   * @throws IOException
   * 
   * 从return out/_temporary/appAttemptId中过滤掉_temporary的文件作为结果返回
   * 获取即该job所有的task目录
   */
  private FileStatus[] getAllCommittedTaskPaths(JobContext context) 
    throws IOException {
    //return out/_temporary/appAttemptId
    Path jobAttemptPath = getJobAttemptPath(context);
    FileSystem fs = jobAttemptPath.getFileSystem(context.getConfiguration());
    return fs.listStatus(jobAttemptPath, new CommittedTaskFilter());
  }
  
  /**
   * Get the directory that the task should write results into.
   * @return the work directory
   * @throws IOException
   */
  public Path getWorkPath() throws IOException {
    return workPath;
  }

  /**
   * Create the temporary directory that is the root of all of the task 
   * work directories.
   * @param context the job's context、
   * 
   * 创建return out/_temporary/appAttemptId目录
   */
  public void setupJob(JobContext context) throws IOException {
    if (hasOutputPath()) {
      Path jobAttemptPath = getJobAttemptPath(context);
      FileSystem fs = jobAttemptPath.getFileSystem(
          context.getConfiguration());
      if (!fs.mkdirs(jobAttemptPath)) {
        LOG.error("Mkdirs failed to create " + jobAttemptPath);
      }
    } else {
      LOG.warn("Output Path is null in setupJob()");
    }
  }
  
  /**
   * The job has completed so move all committed tasks to the final output dir.
   * Delete the temporary directory, including all of the work directories.
   * Create a _SUCCESS file to make it as successful.
   * @param context the job's context
   * 
   * 1.从return out/_temporary/appAttemptId中过滤掉_temporary的文件作为结果返回
   * 2.将完成的文件剪切到目标目录中
   * 3.cleanupJob 清空job,因为job已经完成了
   * 4.
   */
  public void commitJob(JobContext context) throws IOException {
    if (hasOutputPath()) {
      Path finalOutput = getOutputPath();
      FileSystem fs = finalOutput.getFileSystem(context.getConfiguration());
      //从return out/_temporary/appAttemptId中过滤掉_temporary的文件作为结果返回,并且循环
      for(FileStatus stat: getAllCommittedTaskPaths(context)) {
        //将每一个文件剪切到finalOutput目录里面
        mergePaths(fs, stat, finalOutput);
      }

      // delete the _temporary folder and create a _done file in the o/p folder
      cleanupJob(context);
      // True if the job requires output.dir marked on successful job.
      // Note that by default it is set to true.
      //创建_SUCCESS文件,当标示是否成功完成了该job,如果该参数设置为true,则要当job完成后标示该job是完成的,即在该输出目录里面生成一个_SUCCESS的空目录即可
      if (context.getConfiguration().getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
        Path markerPath = new Path(outputPath, SUCCEEDED_FILE_NAME);
        fs.create(markerPath).close();
      }
    } else {
      LOG.warn("Output Path is null in commitJob()");
    }
  }

  /**
   * Merge two paths together.  Anything in from will be moved into to, if there
   * are any name conflicts while merging the files or directories in from win.
   * @param fs the File System to use文件系统
   * @param from the path data is coming from.数据源地址
   * @param to the path data is going to.目标地址
   * @throws IOException on any error
   * 将from文件或者目录剪切到to中,如果遇见任何文件有冲突,则覆盖处理
   */
  private static void mergePaths(FileSystem fs, final FileStatus from,
      final Path to)
    throws IOException {
     LOG.debug("Merging data from "+from+" to "+to);
     if(from.isFile()) {//数据源是文件
       if(fs.exists(to)) {//如果目标地址也有该文件,则要先删除
         if(!fs.delete(to, true)) {
           throw new IOException("Failed to delete "+to);
         }
       }
       //将该数据源文件剪切到目标文件中
       if(!fs.rename(from.getPath(), to)) {
         throw new IOException("Failed to rename "+from+" to "+to);
       }
     } else if(from.isDirectory()) {//是目录
       if(fs.exists(to)) {//目标地址中存在该目录
         FileStatus toStat = fs.getFileStatus(to);
         if(!toStat.isDirectory()) {//如果目标地址中该文件不是目录,则将其删除,然后将原始文件剪切过去即可
           if(!fs.delete(to, true)) {
             throw new IOException("Failed to delete "+to);
           }
           if(!fs.rename(from.getPath(), to)) {
             throw new IOException("Failed to rename "+from+" to "+to);
           }
         } else {//目标文件也是一个目录,因此要做合并
           //It is a directory so merge everything in the directories
           for(FileStatus subFrom: fs.listStatus(from.getPath())) {
             Path subTo = new Path(to, subFrom.getPath().getName());
             mergePaths(fs, subFrom, subTo);
           }
         }
       } else {//目标地址不存在该目录,则直接剪切即可
         //it does not exist just rename
         if(!fs.rename(from.getPath(), to)) {
           throw new IOException("Failed to rename "+from+" to "+to);
         }
       }
     }
  }

  /**
   * job完成后,要清空job临时文件目录
   * 
   * 删除return out/_temporary目录
   */
  @Override
  @Deprecated
  public void cleanupJob(JobContext context) throws IOException {
    if (hasOutputPath()) {
      //获取临时目录return out/_temporary
      Path pendingJobAttemptsPath = getPendingJobAttemptsPath();
      //将临时目录删除
      FileSystem fs = pendingJobAttemptsPath
          .getFileSystem(context.getConfiguration());
      fs.delete(pendingJobAttemptsPath, true);
    } else {
      LOG.warn("Output Path is null in cleanupJob()");
    }
  }

  /**
   * Delete the temporary directory, including all of the work directories.
   * @param context the job's context
   * abortJob算job已经完成,不再继续操作
   * 因此job完成后,要清空job临时文件目录
   */
  @Override
  public void abortJob(JobContext context, JobStatus.State state) 
  throws IOException {
    // delete the _temporary folder
    cleanupJob(context);
  }
  
  /**
   * No task setup required.
   */
  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    // FileOutputCommitter's setupTask doesn't do anything. Because the
    // temporary task directory is created on demand when the 
    // task is writing.
  }

  /**
   * Move the files from the work directory to the job output directory
   * @param context the task context
   * 将在工作目录中存储的文件 剪切到job的输出目录中
   */
  @Override
  public void commitTask(TaskAttemptContext context) 
  throws IOException {
    commitTask(context, null);
  }

  /**
   * 将在工作目录中存储的文件 剪切到job的输出目录中
   */
  @Private
  public void commitTask(TaskAttemptContext context, Path taskAttemptPath) 
  throws IOException {
    TaskAttemptID attemptId = context.getTaskAttemptID();
    if (hasOutputPath()) {
      context.progress();
      if(taskAttemptPath == null) {
        taskAttemptPath = getTaskAttemptPath(context);//out/_temporary/appAttemptId/_temporary/TaskAttemptID 工作目录
      }
      
      /**
        * 获取task的完成后的工作目录
        * return out/_temporary/appAttemptId/TaskAttemptID
       */
      Path committedTaskPath = getCommittedTaskPath(context);
      FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());
      if (fs.exists(taskAttemptPath)) {//如果该完成后的目录存在,则删除他
        if(fs.exists(committedTaskPath)) {
          if(!fs.delete(committedTaskPath, true)) {
            throw new IOException("Could not delete " + committedTaskPath);
          }
        }
        //将该task的工作目录剪切到该task完成后的目录中
        if(!fs.rename(taskAttemptPath, committedTaskPath)) {
          throw new IOException("Could not rename " + taskAttemptPath + " to "
              + committedTaskPath);
        }
        LOG.info("Saved output of task '" + attemptId + "' to " + 
            committedTaskPath);
      } else {
        LOG.warn("No Output found for " + attemptId);
      }
    } else {
      LOG.warn("Output Path is null in commitTask()");
    }
  }

  /**
   * Delete the work directory
   * 删除该task的临时目录,因为该task已经失败了
   * return out/_temporary/appAttemptId/_temporary/TaskAttemptID
   * @throws IOException 
   */
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    abortTask(context, null);
  }

  /**
   * 删除该task的临时目录,因为该task已经失败了
   * return out/_temporary/appAttemptId/_temporary/TaskAttemptID
   */
  @Private
  public void abortTask(TaskAttemptContext context, Path taskAttemptPath) throws IOException {
    if (hasOutputPath()) { 
      context.progress();
      if(taskAttemptPath == null) {
        taskAttemptPath = getTaskAttemptPath(context);
      }
      FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());
      if(!fs.delete(taskAttemptPath, true)) {
        LOG.warn("Could not delete "+taskAttemptPath);
      }
    } else {
      LOG.warn("Output Path is null in abortTask()");
    }
  }

  /**
   * Did this task write any files in the work directory?
   * @param context the task's context
   * 获取该task的工作目录是否存在,存在则返回true,说明可以需要提交
   * return out/_temporary/appAttemptId/_temporary/TaskAttemptID
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context
                                 ) throws IOException {
    return needsTaskCommit(context, null);
  }

  /**
   * 获取该task的工作目录是否存在,存在则返回true,说明可以需要提交
   * return out/_temporary/appAttemptId/_temporary/TaskAttemptID
   */
  @Private
  public boolean needsTaskCommit(TaskAttemptContext context, Path taskAttemptPath
    ) throws IOException {
    if(hasOutputPath()) {
      if(taskAttemptPath == null) {
        taskAttemptPath = getTaskAttemptPath(context);
      }
      FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());
      return fs.exists(taskAttemptPath);
    }
    return false;
  }

  @Override
  @Deprecated
  public boolean isRecoverySupported() {
    return true;
  }
  
  /**
   * 重新执行一个task
   * 该重新执行,不一定是任务失败,而是任务重启,而老任务task可能已经完成了,因此如果是这样情况,只需要剪切老的已经完成的目录到新的task里面来,避免重新跑一次task
   */
  @Override
  public void recoverTask(TaskAttemptContext context)
      throws IOException {
    if(hasOutputPath()) {
      context.progress();
      TaskAttemptID attemptId = context.getTaskAttemptID();
      //获取上一个该task版本号,如果该版本号是第一个,则不允许覆盖
      int previousAttempt = getAppAttemptId(context) - 1;
      if (previousAttempt < 0) {
        throw new IOException ("Cannot recover task output for first attempt...");
      }

      /**
       * 获取上一次task和本次task的完成后的工作目录
       * return out/_temporary/appAttemptId/TaskAttemptID
       */
      Path committedTaskPath = getCommittedTaskPath(context);
      Path previousCommittedTaskPath = getCommittedTaskPath(
          previousAttempt, context);
      FileSystem fs = committedTaskPath.getFileSystem(context.getConfiguration());

      LOG.debug("Trying to recover task from " + previousCommittedTaskPath 
          + " into " + committedTaskPath);
      if (fs.exists(previousCommittedTaskPath)) {//如果上一个版本存在task的完成目录,则将该完成目录结果剪切到新task即可
        if(fs.exists(committedTaskPath)) {
          if(!fs.delete(committedTaskPath, true)) {
            throw new IOException("Could not delete "+committedTaskPath);
          }
        }
        //Rename can fail if the parent directory does not yet exist.
        Path committedParent = committedTaskPath.getParent();
        fs.mkdirs(committedParent);
        if(!fs.rename(previousCommittedTaskPath, committedTaskPath)) {
          throw new IOException("Could not rename " + previousCommittedTaskPath +
              " to " + committedTaskPath);
        }
        LOG.info("Saved output of " + attemptId + " to " + committedTaskPath);
      } else {
        LOG.warn(attemptId+" had no output to recover.");
      }
    } else {
      LOG.warn("Output Path is null in recoverTask()");
    }
  }
}
