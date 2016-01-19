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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.LocatedFileStatusFetcher;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/** 
 * A base class for file-based {@link InputFormat}s.
 * 
 * <p><code>FileInputFormat</code> is the base class for all file-based 
 * <code>InputFormat</code>s. This provides a generic implementation of
 * {@link #getSplits(JobContext)}.
 * Subclasses of <code>FileInputFormat</code> can also override the 
 * {@link #isSplitable(JobContext, Path)} method to ensure input-files are
 * not split-up and are processed as a whole by {@link Mapper}s.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileInputFormat<K, V> extends InputFormat<K, V> {
  public static final String INPUT_DIR = "mapreduce.input.fileinputformat.inputdir";//文件的输入路径,多个文件用逗号拆分
  public static final String SPLIT_MAXSIZE = "mapreduce.input.fileinputformat.split.maxsize";//文件拆分的最大值 
  public static final String SPLIT_MINSIZE = "mapreduce.input.fileinputformat.split.minsize";//文件拆分的最小值 
    
  public static final String PATHFILTER_CLASS = "mapreduce.input.pathFilter.class";//过滤文件的类,可以自定义,也可以默认值
    
  public static final String NUM_INPUT_FILES = "mapreduce.input.fileinputformat.numinputfiles";//最终拆分split之后的文件数据块数量
    
  public static final String INPUT_DIR_RECURSIVE = "mapreduce.input.fileinputformat.input.dir.recursive";//设置文件夹是否要递归读
    
  public static final String LIST_STATUS_NUM_THREADS = "mapreduce.input.fileinputformat.list-status.num-threads";//读取文件集合的线程数
      
  public static final int DEFAULT_LIST_STATUS_NUM_THREADS = 1;//默认线程数,与LIST_STATUS_NUM_THREADS变量关联

  private static final Log LOG = LogFactory.getLog(FileInputFormat.class);

  private static final double SPLIT_SLOP = 1.1;   // 10% slop 最后数据块的1.1被要被存放到最后一个数据块中
  
  @Deprecated
  public static enum Counter { 
    BYTES_READ
  }

  /**
   * 过滤掉以_或者.开头的文件
   */
  private static final PathFilter hiddenFileFilter = new PathFilter(){
      public boolean accept(Path p){
        String name = p.getName(); 
        return !name.startsWith("_") && !name.startsWith("."); 
      }
    }; 

  /**
   * Proxy PathFilter that accepts a path only if all filters given in the
   * constructor do. Used by the listPaths() to apply the built-in
   * hiddenFileFilter together with a user provided one (if any).
   * 存储多个文件过滤器
   */
  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }
  
  /**
   * @param job
   *          the job to modify
   * @param inputDirRecursive
   * 设置文件夹是否要递归读
   */
  public static void setInputDirRecursive(Job job,
      boolean inputDirRecursive) {
    job.getConfiguration().setBoolean(INPUT_DIR_RECURSIVE,
        inputDirRecursive);
  }
 
  /**
   * @param job
   *          the job to look at.
   * @return should the files to be read recursively?
   * 设置文件夹是否要递归读
   */
  public static boolean getInputDirRecursive(JobContext job) {
    return job.getConfiguration().getBoolean(INPUT_DIR_RECURSIVE,
        false);
  }

  /**
   * Get the lower bound on split size imposed by the format.
   * @return the number of bytes of the minimal split for this format
   */
  protected long getFormatMinSplitSize() {
    return 1;
  }

  /**
   * Is the given filename splitable? Usually, true, but if the file is
   * stream compressed, it will not be.
   * 
   * <code>FileInputFormat</code> implementations can override this and return
   * <code>false</code> to ensure that individual input files are never split-up
   * so that {@link Mapper}s process entire files.
   * 
   * @param context the job context
   * @param filename the file name to check
   * @return is this file splitable?
   */
  protected boolean isSplitable(JobContext context, Path filename) {
    return true;
  }

  /**
   * Set the minimum input split size
   * @param job the job to modify
   * @param size the minimum size
   */
  public static void setMinInputSplitSize(Job job,
                                          long size) {
    job.getConfiguration().setLong(SPLIT_MINSIZE, size);
  }

  /**
   * Get the minimum split size
   * @param job the job
   * @return the minimum number of bytes that can be in a split
   */
  public static long getMinSplitSize(JobContext job) {
    return job.getConfiguration().getLong(SPLIT_MINSIZE, 1L);
  }

  /**
   * Set the maximum split size
   * @param job the job to modify
   * @param size the maximum split size
   */
  public static void setMaxInputSplitSize(Job job,
                                          long size) {
    job.getConfiguration().setLong(SPLIT_MAXSIZE, size);
  }

  /**
   * Get the maximum split size.
   * @param context the job to look at.
   * @return the maximum number of bytes a split can include
   */
  public static long getMaxSplitSize(JobContext context) {
    return context.getConfiguration().getLong(SPLIT_MAXSIZE, 
                                              Long.MAX_VALUE);
  }

  /**
   * Set a PathFilter to be applied to the input paths for the map-reduce job.
   * @param job the job to modify
   * @param filter the PathFilter class use for filtering the input paths.
   * 设置文件过滤器
   */
  public static void setInputPathFilter(Job job,
                                        Class<? extends PathFilter> filter) {
    job.getConfiguration().setClass(PATHFILTER_CLASS, filter, 
                                    PathFilter.class);
  }
  
  /**
   * Get a PathFilter instance of the filter set for the input paths.
   *
   * @return the PathFilter instance set for the job, NULL if none has been set.
   * 获取文件过滤器
   */
  public static PathFilter getInputPathFilter(JobContext context) {
    Configuration conf = context.getConfiguration();
    Class<?> filterClass = conf.getClass(PATHFILTER_CLASS, null,
        PathFilter.class);
    return (filterClass != null) ?
        (PathFilter) ReflectionUtils.newInstance(filterClass, conf) : null;
  }

  /** List input directories.
   * Subclasses may override to, e.g., select only files matching a regular
   * expression. 
   * 
   * @param job the job to list input paths for
   * @return array of FileStatus objects
   * @throws IOException if zero items.
   * 获取文件集合
   */
  protected List<FileStatus> listStatus(JobContext job
                                        ) throws IOException {
    Path[] dirs = getInputPaths(job);//输入路径集合
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    
    // get tokens for all the required FileSystems.. 校验权限
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, 
                                        job.getConfiguration());

    // Whether we need to recursive look into the directory structure 是否要递归
    boolean recursive = getInputDirRecursive(job);

    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any). 根据设置,生成文件夹过滤条件
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);
    
    //最终要处理的文件集合
    List<FileStatus> result = null;

    //读取文件集合的线程数
    int numThreads = job.getConfiguration().getInt(LIST_STATUS_NUM_THREADS,DEFAULT_LIST_STATUS_NUM_THREADS);
    Stopwatch sw = new Stopwatch().start();//统计执行时间
    //开始通过过滤条件和输入路径,获取最终要处理的文件集合
    if (numThreads == 1) {
      result = singleThreadedListStatus(job, dirs, inputFilter, recursive);
    } else {
      Iterable<FileStatus> locatedFiles = null;
      try {
        LocatedFileStatusFetcher locatedFileStatusFetcher = new LocatedFileStatusFetcher(job.getConfiguration(), dirs, recursive, inputFilter, true);
        locatedFiles = locatedFileStatusFetcher.getFileStatuses();
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while getting file statuses");
      }
      result = Lists.newArrayList(locatedFiles);
    }
    
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time taken to get FileStatuses: " + sw.elapsedMillis());//打印执行时间
    }
    LOG.info("Total input paths to process : " + result.size()); //输出输入源最终获取的总文件数量
    return result;
  }

  /**
   * 单线程读取文件路径
   * @param job
   * @param dirs 输入路径集合
   * @param inputFilter 对路径进行过滤的条件对象
   * @param recursive是否递归目录
   * @return 
   * @throws IOException
   */
  private List<FileStatus> singleThreadedListStatus(JobContext job, Path[] dirs,
      PathFilter inputFilter, boolean recursive) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();//存储最终过滤后的文件对象
    List<IOException> errors = new ArrayList<IOException>();//存储错误信息
    for (int i=0; i < dirs.length; ++i) {
      Path p = dirs[i];
      FileSystem fs = p.getFileSystem(job.getConfiguration()); 
      FileStatus[] matches = fs.globStatus(p, inputFilter);//通过过滤条件获取文件
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat: matches) {
          if (globStat.isDirectory()) {
            RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(globStat.getPath());
            while (iter.hasNext()) {
              LocatedFileStatus stat = iter.next();
              if (inputFilter.accept(stat.getPath())) {
                if (recursive && stat.isDirectory()) {
                  addInputPathRecursively(result, fs, stat.getPath(),inputFilter);
                } else {
                  result.add(stat);
                }
              }
            }
          } else {//是文件则添加
            result.add(globStat);
          }
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    return result;
  }
  
  /**
   * Add files in the input path recursively into the results.
   * @param result
   *          The List to store all files.
   * @param fs
   *          The FileSystem.
   * @param path
   *          The input path.
   * @param inputFilter
   *          The input filter that can be used to filter files/dirs. 
   * @throws IOException
   */
  protected void addInputPathRecursively(List<FileStatus> result,
      FileSystem fs, Path path, PathFilter inputFilter) 
      throws IOException {
    RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
    while (iter.hasNext()) {
      LocatedFileStatus stat = iter.next();
      if (inputFilter.accept(stat.getPath())) {
        if (stat.isDirectory()) {
          addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
        } else {
          result.add(stat);
        }
      }
    }
  }
  
  
  /**
   * A factory that makes the split for this class. It can be overridden
   * by sub-classes to make sub-types
   */
  protected FileSplit makeSplit(Path file, long start, long length, 
                                String[] hosts) {
    return new FileSplit(file, start, length, hosts);
  }
  
  /**
   * A factory that makes the split for this class. It can be overridden
   * by sub-classes to make sub-types
   */
  protected FileSplit makeSplit(Path file, long start, long length, 
                                String[] hosts, String[] inMemoryHosts) {
    return new FileSplit(file, start, length, hosts, inMemoryHosts);
  }

  /** 
   * Generate the list of files and make them into FileSplits.
   * @param job the job context
   * @throws IOException
   */
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    Stopwatch sw = new Stopwatch().start();//计算时间
    long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
    long maxSize = getMaxSplitSize(job);

    // generate splits
    List<InputSplit> splits = new ArrayList<InputSplit>();//最终拆分结果
    List<FileStatus> files = listStatus(job);//获取文件集合
    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();//获取文件大小
      if (length != 0) {
        BlockLocation[] blkLocations;//获取该数据块的位置集合
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          FileSystem fs = path.getFileSystem(job.getConfiguration());
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        if (isSplitable(job, path)) {//是要拆分的
          long blockSize = file.getBlockSize();//数据块大小
          long splitSize = computeSplitSize(blockSize, minSize, maxSize);//计算拆分大小

          long bytesRemaining = length;
          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
            int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
            splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                        blkLocations[blkIndex].getHosts(),
                        blkLocations[blkIndex].getCachedHosts()));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {//生成最后一个数据块
            //根据偏移量,查看该偏移量在哪个数据块信息中
            int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
            splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining,
                       blkLocations[blkIndex].getHosts(),//该数据块所在的host集合
                       blkLocations[blkIndex].getCachedHosts()));//该数据块所在的缓存集合
          }
        } else { // not splitable 不进行拆分,则
          splits.add(makeSplit(path, 0, length, blkLocations[0].getHosts(),
                      blkLocations[0].getCachedHosts()));
        }
      } else {//说明该文件没有大小,因此生成一个空文件,同时所在节点也为空
        //Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    // Save the number of input files for metrics/loadgen 设置最终要多少个map处理的数据块数
    job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size()
          + ", TimeTaken: " + sw.elapsedMillis());
    }
    return splits;
  }

  protected long computeSplitSize(long blockSize, long minSize,
                                  long maxSize) {
    return Math.max(minSize, Math.min(maxSize, blockSize));
  }

  /**
   * 从所有被拆分的数据块中查找偏移量所在的数据块位置
   * @param blkLocations 该文件所有的数据块集合
   * @param offset 要获取的数据块在整个文件的偏移量
   * @return
   */
  protected int getBlockIndex(BlockLocation[] blkLocations, 
                              long offset) {
    for (int i = 0 ; i < blkLocations.length; i++) {//在文件的所有数据块中查找包含偏移量为offset的数据块
      // is the offset inside this block?
      if ((blkLocations[i].getOffset() <= offset) &&
          (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())){
        return i;
      }
    }
    //如果程序到这里,说明没有找到偏移量信息,因此出异常了
    BlockLocation last = blkLocations[blkLocations.length -1];//获取包含该偏移量的数据块信息，包含该数据块所在服务器集合等信息
    long fileLength = last.getOffset() + last.getLength() -1;
    throw new IllegalArgumentException("Offset " + offset + 
                                       " is outside of file (0.." +
                                       fileLength + ")");
  }

  /**
   * Sets the given comma separated paths as the list of inputs 
   * for the map-reduce job.
   * 
   * @param job the job
   * @param commaSeparatedPaths Comma separated paths to be set as 
   *        the list of inputs for the map-reduce job.
   */
  public static void setInputPaths(Job job, 
                                   String commaSeparatedPaths
                                   ) throws IOException {
    setInputPaths(job, StringUtils.stringToPath(
                        getPathStrings(commaSeparatedPaths)));
  }

  /**
   * Set the array of {@link Path}s as the list of inputs
   * for the map-reduce job.
   * 
   * @param job The job to modify 
   * @param inputPaths the {@link Path}s of the input directories/files 
   * for the map-reduce job.
   * 重置输入的路径,以前的设置被清空
   */ 
  public static void setInputPaths(Job job, 
                                   Path... inputPaths) throws IOException {
    Configuration conf = job.getConfiguration();
    Path path = inputPaths[0].getFileSystem(conf).makeQualified(inputPaths[0]);
    StringBuffer str = new StringBuffer(StringUtils.escapeString(path.toString()));
    for(int i = 1; i < inputPaths.length;i++) {
      str.append(StringUtils.COMMA_STR);
      path = inputPaths[i].getFileSystem(conf).makeQualified(inputPaths[i]);
      str.append(StringUtils.escapeString(path.toString()));
    }
    conf.set(INPUT_DIR, str.toString());
  }

  /**
   * Add the given comma separated paths to the list of inputs for
   *  the map-reduce job.
   * 
   * @param job The job to modify
   * @param commaSeparatedPaths Comma separated paths to be added to
   *        the list of inputs for the map-reduce job.
   * 添加输入原路径,多个输入源由逗号分割
   * 添加多个输入路径
   */
  public static void addInputPaths(Job job, 
                                   String commaSeparatedPaths
                                   ) throws IOException {
    for (String str : getPathStrings(commaSeparatedPaths)) {
      addInputPath(job, new Path(str));
    }
  }

  /**
   * Add a {@link Path} to the list of inputs for the map-reduce job.
   * 
   * @param job The {@link Job} to modify
   * @param path {@link Path} to be added to the list of inputs for 
   *            the map-reduce job.
   * 用逗号分割输入路径
   * 添加一个数据路径
   */
  public static void addInputPath(Job job, 
                                  Path path) throws IOException {
    Configuration conf = job.getConfiguration();
    path = path.getFileSystem(conf).makeQualified(path);
    String dirStr = StringUtils.escapeString(path.toString());
    String dirs = conf.get(INPUT_DIR);
    conf.set(INPUT_DIR, dirs == null ? dirStr : dirs + "," + dirStr);
  }
  
  // This method escapes commas in the glob pattern of the given paths.
  /**
   * 按照逗号拆分,但是注意,当在{}括号中的逗号是不会被拆分的,会跟随{}一起输出
   * 例如:参数adsa,dsa{dd,dd}dsa,{sd}d
   * 输出
   * adsa
      dsa{dd,dd}dsa
      {sd}d
   */
  private static String[] getPathStrings(String commaSeparatedPaths) {
    int length = commaSeparatedPaths.length();
    int curlyOpen = 0;
    int pathStart = 0;
    boolean globPattern = false;
    List<String> pathStrings = new ArrayList<String>();
    
    for (int i=0; i<length; i++) {
      char ch = commaSeparatedPaths.charAt(i);
      switch(ch) {
        case '{' : {
          curlyOpen++;
          if (!globPattern) {
            globPattern = true;
          }
          break;
        }
        case '}' : {
          curlyOpen--;
          if (curlyOpen == 0 && globPattern) {
            globPattern = false;
          }
          break;
        }
        case ',' : {
          if (!globPattern) {
            pathStrings.add(commaSeparatedPaths.substring(pathStart, i));
            pathStart = i + 1 ;
          }
          break;
        }
        default:
          continue; // nothing special to do for this character
      }
    }
    pathStrings.add(commaSeparatedPaths.substring(pathStart, length));
    
    return pathStrings.toArray(new String[0]);
  }
  
  /**
   * Get the list of input {@link Path}s for the map-reduce job.
   * 
   * @param context The job
   * @return the list of input {@link Path}s for the map-reduce job.
   * 多个文件用逗号拆分
   * 获取输入源所有路径集合
   */
  public static Path[] getInputPaths(JobContext context) {
    String dirs = context.getConfiguration().get(INPUT_DIR, "");
    String [] list = StringUtils.split(dirs);//按照逗号拆分字符串
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }

}
