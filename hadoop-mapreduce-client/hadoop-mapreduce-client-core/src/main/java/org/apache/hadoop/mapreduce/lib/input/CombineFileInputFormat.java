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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.NetworkTopology;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * An abstract {@link InputFormat} that returns {@link CombineFileSplit}'s in 
 * {@link InputFormat#getSplits(JobContext)} method. 
 * 
 * Splits are constructed from the files under the input paths. 
 * A split cannot have files from different pools.
 * Each split returned may contain blocks from different files.
 *
 *
 * If a maxSplitSize is specified, then blocks on the same node are
 * combined to form a single split. Blocks that are left over are
 * then combined with other blocks in the same rack.
 * 如果maxSplitSize设置了,则在相同node上的数据块会拆分成一个分片,余下的数据块和其他数据块在相同的rack上合并
 *
 *
 * If maxSplitSize is not specified, then blocks from the same rack
 * are combined in a single split; no attempt is made to create
 * node-local splits.
 *
 * If the maxSplitSize is equal to the block size, then this class
 * is similar to the default splitting behavior in Hadoop: each
 * block is a locally processed split.
 * Subclasses implement 
 * {@link InputFormat#createRecordReader(InputSplit, TaskAttemptContext)}
 * to construct <code>RecordReader</code>'s for 
 * <code>CombineFileSplit</code>'s.
 * 
 * @see CombineFileSplit
 * 目标是让一个map处理多个数据块以及这些数据块可能来自与不同的文件
 * 主要是输入源都是小文件的时候可以尝试使用这种方式
 *
 *
 *
 * CombineInputFormat处理少量，较大的文件没有优势，相反，如果没有合理的设置maxSplitSize，minSizeNode，minSizeRack，则可能会导致一个map任务需要大量访问非本地的Block造成网络开销，反而比正常的非合并方式更慢。
 * 因为本来可以在本地读取一个数据块信息的,却要读取多个文件的数据块信息,因此需要网络开销,有利弊要取舍以及测试,合理设置这三个size属性值很关键
   而针对大量远小于块大小的小文件处理，CombineInputFormat的使用还是很有优势。

 我自己总结的分配规则--详细参见createSplits方法
一、第一个while循环,先为每一个节点分配一个分片
1.循环每一个有数据块的节点
2.循环该节点上所有的数据块
如果设置maxSize,并且数据块的字节总数超过了maxSize,则创建一个分片,不再为该数据块分配分片
如果没设置maxSize,但是设置了minSizeNode,并且该节点上的数据块超过了minSizeNode,说明该节点上数据块可以拆分成一个大的分片
如果没设置maxSize,但是设置了minSizeNode,并且该节点上的数据块小于minSizeNode,说明该节点上数据块太小了,不足分配一个分片,
则不会在该节点创建任意分片,都会给rack去创建
如果没设置maxSize,也没有设置minSizeNode,则不会在该节点创建任意分片,都会给rack去创建

二、第二个while循环,为每一个rack分配分片,为每一个rack分配一个分片
1.循环每一个rack
2.循环该rack上所有的数据块,过滤尚未被分配的数据块
3.如果设置maxSize,并且数据块的字节总数超过了maxSize,则在该rack上创建一个分片,不再为该数据块分配分片
如果没设置maxSize,但是设置了minSizeRack,并且该rack上的数据块超过了minSizeRack,说明该rack上数据块可以拆分成一个大的分片
如果没设置maxSize,但是设置了minSizeRack,并且该rack上的数据块小于minSizeRack,说明该rack上数据块太小了,不足分配一个分片,
则不会在该rack创建任意分片,都会给overflowBlocks去保存尚未被分配的数据块,最后整体去分配
如果没设置maxSize,也没有设置minSizeRack,则不会在该rack创建任意分片,都会给overflowBlocks去保存尚未被分配的数据块,最后整体去分配
三、统一分配剩余的数据块
1.循环overflowBlocks集合,即循环尚未被分配的数据块集合
2.如果设置maxSize,并且数据块的字节总数超过了maxSize,则在该数据块集合存在的节点上创建一个分片,并且不断循环创建分片
如果没设置maxSize,则将所有剩余的数据块都生成一个大的分片,有且只有一个。


汇总--三个属性产生8中组合
maxSize
minSizeNode
minSizeRack


1.仅仅设置maxSize   设置了maxSize和minSizeNode    设置了maxSize和minSizeRack  设置了maxSize和minSizeNode和minSizeRack
则效果一样,都是不关注minSizeNode和minSizeRack  --基本上每一个分片都是maxSize大小
每一个节点上达到字节数量的都会产生一个分片
每一个rack上达到数量的也会产生一个分片
不断的在rack上产生分片

2.仅仅设置了minSizeNode,--产生node节点数量+一个大的分片
每一个节点上达到minSizeNode的,就会产生一个大的分片,该节点上不会到rack上分配
如果没有达到minSizeNode,则会去rack上分配,最后所有没有达到minSizeNode的就会产生一个大的数据块

3.仅仅设置了minSizeRack  --rack节点数量+一个大的分片
每一个rack上达到minSizeRack的,就会产生一个大的分片
如果没有达到minSizeRack,则会去overflowBlocks上分配
overflowBlocks上最终会形成一个大的分片
4.设置了minSizeNode和minSizeRack  --产生node节点数量+rack节点数量+一个大的分片
每一个节点上达到minSizeNode的,就会产生一个大的分片,该节点上不会到rack上分配
如果没有达到minSizeNode,则会去rack上分配

每一个rack上达到minSizeRack的,就会产生一个大的分片
如果没有达到minSizeRack,则会去overflowBlocks上分配
overflowBlocks上最终会形成一个大的分片

5.三个属性什么都没设置   --一个大的分片
只会生成一个大的数据分片,包含全部数据内容
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CombineFileInputFormat<K, V>
  extends FileInputFormat<K, V> {
  
  private static final Log LOG = LogFactory.getLog(CombineFileInputFormat.class);
  
  public static final String SPLIT_MINSIZE_PERNODE = 
    "mapreduce.input.fileinputformat.split.minsize.per.node";
  public static final String SPLIT_MINSIZE_PERRACK = 
    "mapreduce.input.fileinputformat.split.minsize.per.rack";
  // ability to limit the size of a single split
  private long maxSplitSize = 0;
  private long minSplitSizeNode = 0;
  private long minSplitSizeRack = 0;

  // A pool of input paths filters. A split cannot have blocks from files
  // across multiple pools.
  //一个输入源文件路径的过滤集合,
  private ArrayList<MultiPathFilter> pools = new  ArrayList<MultiPathFilter>();

  // mapping from a rack name to the set of Nodes in the rack
  //一个rack上有哪些备份节点集合
  private HashMap<String, Set<String>> rackToNodes = 
                            new HashMap<String, Set<String>>();
  /**
   * Specify the maximum size (in bytes) of each split. Each split is
   * approximately equal to the specified size.
   */
  protected void setMaxSplitSize(long maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
  }

  /**
   * Specify the minimum size (in bytes) of each split per node.
   * This applies to data that is left over after combining data on a single
   * node into splits that are of maximum size specified by maxSplitSize.
   * This leftover data will be combined into its own split if its size
   * exceeds minSplitSizeNode.
   */
  protected void setMinSplitSizeNode(long minSplitSizeNode) {
    this.minSplitSizeNode = minSplitSizeNode;
  }

  /**
   * Specify the minimum size (in bytes) of each split per rack.
   * This applies to data that is left over after combining data on a single
   * rack into splits that are of maximum size specified by maxSplitSize.
   * This leftover data will be combined into its own split if its size
   * exceeds minSplitSizeRack.
   */
  protected void setMinSplitSizeRack(long minSplitSizeRack) {
    this.minSplitSizeRack = minSplitSizeRack;
  }

  /**
   * Create a new pool and add the filters to it.
   * A split cannot have files from different pools.
   */
  protected void createPool(List<PathFilter> filters) {
    pools.add(new MultiPathFilter(filters));
  }

  /**
   * Create a new pool and add the filters to it. 
   * A pathname can satisfy any one of the specified filters.
   * A split cannot have files from different pools.
   */
  protected void createPool(PathFilter... filters) {
    MultiPathFilter multi = new MultiPathFilter();
    for (PathFilter f: filters) {
      multi.add(f);
    }
    pools.add(multi);
  }
  
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    final CompressionCodec codec =
      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }

  /**
   * default constructor
   */
  public CombineFileInputFormat() {
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) 
    throws IOException {
    long minSizeNode = 0;
    long minSizeRack = 0;
    long maxSize = 0;
    Configuration conf = job.getConfiguration();

    // the values specified by setxxxSplitSize() takes precedence over the
    // values that might have been specified in the config
    if (minSplitSizeNode != 0) {
      minSizeNode = minSplitSizeNode;
    } else {
      minSizeNode = conf.getLong(SPLIT_MINSIZE_PERNODE, 0);
    }
    if (minSplitSizeRack != 0) {
      minSizeRack = minSplitSizeRack;
    } else {
      minSizeRack = conf.getLong(SPLIT_MINSIZE_PERRACK, 0);
    }
    if (maxSplitSize != 0) {
      maxSize = maxSplitSize;
    } else {
      maxSize = conf.getLong("mapreduce.input.fileinputformat.split.maxsize", 0);//文件拆分的最大值
      // If maxSize is not configured, a single split will be generated per
      // node.
    }
    if (minSizeNode != 0 && maxSize != 0 && minSizeNode > maxSize) {
      throw new IOException("Minimum split size pernode " + minSizeNode +
                            " cannot be larger than maximum split size " +
                            maxSize);
    }
    if (minSizeRack != 0 && maxSize != 0 && minSizeRack > maxSize) {
      throw new IOException("Minimum split size per rack " + minSizeRack +
                            " cannot be larger than maximum split size " +
                            maxSize);
    }
    if (minSizeRack != 0 && minSizeNode > minSizeRack) {
      throw new IOException("Minimum split size per node " + minSizeNode +
                            " cannot be larger than minimum split " +
                            "size per rack " + minSizeRack);
    }

    // all the files in input set 获取输入源对应的文件集合
    List<FileStatus> stats = listStatus(job);
    
    //最终拆分好后的组
    List<InputSplit> splits = new ArrayList<InputSplit>();
    if (stats.size() == 0) {
      return splits;    
    }

    //循环所有的stats文件,找到第一个匹配规则匹配的元素
    // In one single iteration, process all the paths in a single pool.
    // Processing one pool at a time ensures that a split contains paths
    // from a single pool only.
    for (MultiPathFilter onepool : pools) {//循环所有的匹配规则
    	//该匹配规则匹配的元素
      ArrayList<FileStatus> myPaths = new ArrayList<FileStatus>();
      
      // pick one input path. If it matches all the filters in a pool,
      // add it to the output set
      for (Iterator<FileStatus> iter = stats.iterator(); iter.hasNext();) {
        FileStatus p = iter.next();
        if (onepool.accept(p.getPath())) {//说明该文件符合该匹配规则
          myPaths.add(p); // add it to my output set  加入到输出文件中
          iter.remove();//该文件以及匹配完成,移除
        }
      }
      // create splits for all files in this pool.在这个池子中,匹配的文件进行split合并拆分
      getMoreSplits(job, myPaths, maxSize, minSizeNode, minSizeRack, splits);
    }

    // create splits for all files that are not in any pool.对不在任何匹配规则池里面的文件进行合并拆分
    getMoreSplits(job, stats, maxSize, minSizeNode, minSizeRack, splits);

    // free up rackToNodes map
    rackToNodes.clear();
    return splits;    
  }

  /**
   * Return all the splits in the specified set of paths
   */
  private void getMoreSplits(JobContext job, List<FileStatus> stats,
                             long maxSize, long minSizeNode, long minSizeRack,
                             List<InputSplit> splits)
    throws IOException {
    Configuration conf = job.getConfiguration();

    // all blocks for all the files in input set 每一个文件对应一个该对象
    OneFileInfo[] files;

    /**
     * 添加映射信息
     1.数据块在哪些节点上存在备份
     2.rack上有哪些数据块
     3.一个node节点上有哪些数据块
     4.一个rack上有哪些备份节点集合
     */
    // mapping from a rack name to the list of blocks it has
    HashMap<String, List<OneBlockInfo>> rackToBlocks = 
                              new HashMap<String, List<OneBlockInfo>>();

    // mapping from a block to the nodes on which it has replicas
    HashMap<OneBlockInfo, String[]> blockToNodes = 
                              new HashMap<OneBlockInfo, String[]>();

    // mapping from a node to the list of blocks that it contains
    HashMap<String, Set<OneBlockInfo>> nodeToBlocks = 
                              new HashMap<String, Set<OneBlockInfo>>();

    //代表一个HDFS上的path路径对应的文件--该文件包含多个数据块
    files = new OneFileInfo[stats.size()];
    if (stats.size() == 0) {
      return; 
    }

    // populate all the blocks for all files
    long totLength = 0;//所有文件的总字节长度
    int i = 0;
    
    //计算所有字节的长度
    for (FileStatus stat : stats) {
      files[i] = new OneFileInfo(stat, conf, isSplitable(job, stat.getPath()),
                                 rackToBlocks, blockToNodes, nodeToBlocks,
                                 rackToNodes, maxSize);
      totLength += files[i].getLength();
    }
    createSplits(nodeToBlocks, blockToNodes, rackToBlocks, totLength, 
                 maxSize, minSizeNode, minSizeRack, splits);
  }

  @VisibleForTesting
  void createSplits(Map<String, Set<OneBlockInfo>> nodeToBlocks,//每一个节点上存储哪些数据块
                     Map<OneBlockInfo, String[]> blockToNodes,//每一个数据块在哪个节点存在
                     Map<String, List<OneBlockInfo>> rackToBlocks,//每一个rack上存储哪些数据块
                     long totLength,//所有数据块的总长度
                     long maxSize,
                     long minSizeNode,
                     long minSizeRack,
                     List<InputSplit> splits//最终存储分片的对象
                    ) {

    //暂时有效的数据块
    ArrayList<OneBlockInfo> validBlocks = new ArrayList<OneBlockInfo>();
    long curSplitSize = 0;//当前validBlocks中数据块字节长度
    
    int totalNodes = nodeToBlocks.size();//总共数据块都在几个节点上存在
    long totalLength = totLength;//所有数据块的剩余总长度

    Multiset<String> splitsPerNode = HashMultiset.create();//在哪些节点上有合并后的数据块

    //说明该节点已经彻底处理完,没有数据块了
    Set<String> completedNodes = new HashSet<String>();

    //while 循环开始
    while(true) {
      // it is allowed for maxSize to be 0. Disable smoothing load for such cases

      // process all nodes and create splits that are local to a node. Generate
      // one split per node iteration, and walk over nodes multiple times to
      // distribute the splits across nodes. 
      for (Iterator<Map.Entry<String, Set<OneBlockInfo>>> iter = nodeToBlocks
          .entrySet().iterator(); iter.hasNext();) {//循环每一个节点
        Map.Entry<String, Set<OneBlockInfo>> one = iter.next();
        
        String node = one.getKey();//节点
        
        // Skip the node if it has previously been marked as completed.
        if (completedNodes.contains(node)) {//基本上不会走这里面
          continue;
        }

        Set<OneBlockInfo> blocksInCurrentNode = one.getValue();//该节点上存储的数据块集合

        // for each block, copy it into validBlocks. Delete it from
        // blockToNodes so that the same block does not appear in
        // two different splits.
        //避免相同的数据块被拆分成两次处理
        /**
         * 本次循环该节点上所有数据块,分两种情况
         * 1.设置maxSize,因此在本节点上分配的数据块达到maxSize时候,则生成一个拆分文件,然后不会在该节点上生成第二个数据块了
         * 2.如果没有设置maxSize,则将全部信息汇总到validBlocks集合中
         */
        Iterator<OneBlockInfo> oneBlockIter = blocksInCurrentNode.iterator();//循环该节点上每一个数据块
        while (oneBlockIter.hasNext()) {
          OneBlockInfo oneblock = oneBlockIter.next();//数据块对象
          
          // Remove all blocks which may already have been assigned to other
          // splits.移除所有已经被分配到其他分片中的数据块
          if(!blockToNodes.containsKey(oneblock)) {//说明该数据块已经被其他节点分配完了,因为一个数据块在多个节点都有备份
            oneBlockIter.remove();//不处理该数据块
            continue;
          }
        
          validBlocks.add(oneblock);
          blockToNodes.remove(oneblock);//移除该数据块,因为已经处理完了
          curSplitSize += oneblock.length;

          // if the accumulated split size exceeds the maximum, then
          // create this split.
          if (maxSize != 0 && curSplitSize >= maxSize) {//说明可以拆分成一个数据块了
            // create an input split and add it to the splits array
            addCreatedSplit(splits, Collections.singleton(node), validBlocks);
            totalLength -= curSplitSize;
            curSplitSize = 0;

            splitsPerNode.add(node);

            // Remove entries from blocksInNode so that we don't walk these
            // again.不需要在分配了,因此移除这些数据块
            blocksInCurrentNode.removeAll(validBlocks);
            validBlocks.clear();

            // Done creating a single split for this node. Move on to the next
            // node so that splits are distributed across nodes.
            //在该节点上仅会创建一个单独的分片,移除到下一个节点上,因此分片会被部署在多个节点上
            break;
          }
        }
        if (validBlocks.size() != 0) {
          //暗示所有的数据块不是一个分片,这个节点完成
          // This implies that the last few blocks (or all in case maxSize=0)
          // were not part of a split. The node is complete.
          
          // if there were any blocks left over and their combined size is
          // larger than minSplitNode, then combine them into one split.
          //如何所以数据块的组合大小,超过了minSplitNode,则和并他们成一个分片
          // Otherwise add them back to the unprocessed pool. It is likely
          // that they will be combined with other blocks from the
          // same rack later on.
          //否则将这些数据块重新设置成为分配,他将会被rack上其他节点合并
          // This condition also kicks in when max split size is not set. All
          // blocks on a node will be grouped together into a single split.
          if (minSizeNode != 0 && curSplitSize >= minSizeNode
              && splitsPerNode.count(node) == 0) {//如果文件大小已经超过了最小聚合的阀值,因此将该文件都在该节点上分片
            // haven't created any split on this machine. so its ok to add a
            // smaller one for parallelism. Otherwise group it in the rack for
            // balanced size create an input split and add it to the splits
            // array
            addCreatedSplit(splits, Collections.singleton(node), validBlocks);
            totalLength -= curSplitSize;
            splitsPerNode.add(node);
            // Remove entries from blocksInNode so that we don't walk this again.
            blocksInCurrentNode.removeAll(validBlocks);
            // The node is done. This was the last set of blocks for this node.
          } else {
            // Put the unplaced blocks back into the pool for later rack-allocation.
            //重新放回去,让后续的rack处理
            for (OneBlockInfo oneblock : validBlocks) {
              blockToNodes.put(oneblock, oneblock.hosts);
            }
          }
          validBlocks.clear();
          curSplitSize = 0;
          completedNodes.add(node);
        } else { // No in-flight blocks.
          if (blocksInCurrentNode.size() == 0) {//说明该节点已经彻底处理完,没有数据块了
            // Node is done. All blocks were fit into node-local splits.
            completedNodes.add(node);
          } // else Run through the node again.
        }
      }

      // Check if node-local assignments are complete.校验本地节点是否都已经全部分配完成
      if (completedNodes.size() == totalNodes || totalLength == 0) {
        // All nodes have been walked over and marked as completed or all blocks
        // have been assigned. The rest should be handled via rackLock assignment.
        LOG.info("DEBUG: Terminated node allocation with : CompletedNodes: "
            + completedNodes.size() + ", size left: " + totalLength);
        break;
      }
    }
    //while 循环结束

    // if blocks in a rack are below the specified minimum size, then keep them
    // in 'overflow'. After the processing of all racks is complete, these 
    // overflow blocks will be combined into splits.
    //还会保留一些数据可--本应该数据该rack,但是由于字节太小,暂时尚不分配
    ArrayList<OneBlockInfo> overflowBlocks = new ArrayList<OneBlockInfo>();
    Set<String> racks = new HashSet<String>();//所有的rack集合

    // Process all racks over and over again until there is no more work to do.
    //处理所有rack
    //while 循环开始
    while (blockToNodes.size() > 0) {//处理剩余的数据块

      // Create one split for this rack before moving over to the next rack. 
      // Come back to this rack after creating a single split for each of the 
      // remaining racks.
      // Process one rack location at a time, Combine all possible blocks that
      // reside on this rack as one split. (constrained by minimum and maximum
      // split size).

      // iterate over all racks 
      for (Iterator<Map.Entry<String, List<OneBlockInfo>>> iter = 
           rackToBlocks.entrySet().iterator(); iter.hasNext();) {//循环每一个rack

        Map.Entry<String, List<OneBlockInfo>> one = iter.next();
        racks.add(one.getKey());
        List<OneBlockInfo> blocks = one.getValue();//该rack上分配的数据块

        // for each block, copy it into validBlocks. Delete it from 
        // blockToNodes so that the same block does not appear in 
        // two different splits.
        boolean createdSplit = false;//true表示已经拆分成一个数据块在该rack上
        for (OneBlockInfo oneblock : blocks) {
          if (blockToNodes.containsKey(oneblock)) {//说明数据块还有效
            validBlocks.add(oneblock);//设置有效的数据块
            blockToNodes.remove(oneblock);
            curSplitSize += oneblock.length;
      
            // if the accumulated split size exceeds the maximum, then 
            // create this split.
            if (maxSize != 0 && curSplitSize >= maxSize) {
              // create an input split and add it to the splits array
              addCreatedSplit(splits, getHosts(racks), validBlocks);
              createdSplit = true;
              break;
            }
          }
        }

        // if we created a split, then just go to the next rack
        if (createdSplit) {
          curSplitSize = 0;
          validBlocks.clear();
          racks.clear();
          continue;
        }

        if (!validBlocks.isEmpty()) {//rack上有有效的数据块
          if (minSizeRack != 0 && curSplitSize >= minSizeRack) {//大于最小的rack
            // if there is a minimum size specified, then create a single split
            // otherwise, store these blocks into overflow data structure
            addCreatedSplit(splits, getHosts(racks), validBlocks);//将所有数据块在rack上分配一个分片
          } else {
            // There were a few blocks in this rack that 
        	// remained to be processed. Keep them in 'overflow' block list. 
        	// These will be combined later.
            //还会保留一些数据可--本应该数据该rack,但是由于字节太小,暂时尚不分配
            overflowBlocks.addAll(validBlocks);
          }
        }
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }
    //while 循环结束

    assert blockToNodes.isEmpty();
    assert curSplitSize == 0;
    assert validBlocks.isEmpty();
    assert racks.isEmpty();

    // Process all overflow blocks
    for (OneBlockInfo oneblock : overflowBlocks) {//循环每一个数据块
      validBlocks.add(oneblock);
      curSplitSize += oneblock.length;

      // This might cause an exiting rack location to be re-added,
      // but it should be ok.
      for (int i = 0; i < oneblock.racks.length; i++) {//该数据块所属rack
        racks.add(oneblock.racks[i]);
      }

      // if the accumulated split size exceeds the maximum, then 
      // create this split.
      if (maxSize != 0 && curSplitSize >= maxSize) {//超过阀值,则创建一个分片
        // create an input split and add it to the splits array
        addCreatedSplit(splits, getHosts(racks), validBlocks);
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }

    // Process any remaining blocks, if any.如果还有数据块,则创建一个分片
    if (!validBlocks.isEmpty()) {
      addCreatedSplit(splits, getHosts(racks), validBlocks);
    }
  }

  /**
   * Create a single split from the list of blocks specified in validBlocks
   * Add this new split into splitList.
   * 真正产生一个数据块--该数据块包含多个文件
   *
   */
  private void addCreatedSplit(List<InputSplit> splitList,//用于存储最终产生的数据块集合
                               Collection<String> locations, //该数据块都在哪些节点上存在
                               ArrayList<OneBlockInfo> validBlocks) {//该合并的数据块要包含哪些真的数据块
    // create an input split
    Path[] fl = new Path[validBlocks.size()];
    long[] offset = new long[validBlocks.size()];
    long[] length = new long[validBlocks.size()];
    for (int i = 0; i < validBlocks.size(); i++) {
      fl[i] = validBlocks.get(i).onepath; 
      offset[i] = validBlocks.get(i).offset;
      length[i] = validBlocks.get(i).length;
    }
     // add this split to the list that is returned
    CombineFileSplit thissplit = new CombineFileSplit(fl, offset, 
                                   length, locations.toArray(new String[0]));
    splitList.add(thissplit); 
  }

  /**
   * This is not implemented yet. 
   */
  public abstract RecordReader<K, V> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException;

  /**
   * information about one file from the File System
   * 代表一个HDFS上的path路径对应的文件--该文件包含多个数据块
   */
  @VisibleForTesting
  static class OneFileInfo {
    private long fileSize;               // size of the file该文件所有的数据块总大小
    private OneBlockInfo[] blocks;       // all blocks in this file 该文件对应的所有数据块集合,每一个对象代表一个数据块,该数据块可能是原始数据块又进一步拆分成更小的数据块

    OneFileInfo(FileStatus stat, Configuration conf,
                boolean isSplitable,
                HashMap<String, List<OneBlockInfo>> rackToBlocks,
                HashMap<OneBlockInfo, String[]> blockToNodes,
                HashMap<String, Set<OneBlockInfo>> nodeToBlocks,
                HashMap<String, Set<String>> rackToNodes,
                long maxSize)
                throws IOException {
      this.fileSize = 0;

      // get block locations from file system
      BlockLocation[] locations;//属于该文件的所有数据块集合
      if (stat instanceof LocatedFileStatus) {
        locations = ((LocatedFileStatus) stat).getBlockLocations();
      } else {
    	  //获取该文件所有的数据块集合
        FileSystem fs = stat.getPath().getFileSystem(conf);
        locations = fs.getFileBlockLocations(stat, 0, stat.getLen());
      }
      // create a list of all block and their locations
      if (locations == null) {//该文件没有数据块
        blocks = new OneBlockInfo[0];//数组长度为0
      } else {

        if(locations.length == 0 && !stat.isDirectory()) {
          locations = new BlockLocation[] { new BlockLocation() };
        }

        if (!isSplitable) {//不可拆分的数据块
          // if the file is not splitable, just create the one block with
          // full file length 说明一个数据块对应全部文件内容
          blocks = new OneBlockInfo[1];//数组长度为1
          fileSize = stat.getLen();//文件大小
          blocks[0] = new OneBlockInfo(stat.getPath(), 0, fileSize,
              locations[0].getHosts(), locations[0].getTopologyPaths());
        } else {
        	//数据块集合
          ArrayList<OneBlockInfo> blocksList = new ArrayList<OneBlockInfo>(
              locations.length);
          for (int i = 0; i < locations.length; i++) {
            fileSize += locations[i].getLength();//所有数据块大小

            // each split can be a maximum of maxSize
            long left = locations[i].getLength();//数据筷大小
            long myOffset = locations[i].getOffset();//数据开始偏移位置
            long myLength = 0;
            do {
              if (maxSize == 0) {//如果没有设置maxSize,则myLength的大小就是这个数据块大小
                myLength = left;
              } else {
                if (left > maxSize && left < 2 * maxSize) {//如果数据块大小在maxSize和2倍maxSize之间,则将该数据块平均拆分成两个,防止一个数据块过于小,导致一个map处理时间长，一个处理时间短
                  // if remainder is between max and 2*max - then
                  // instead of creating splits of size max, left-max we
                  // create splits of size left/2 and left/2. This is
                  // a heuristic to avoid creating really really small
                  // splits.
                  myLength = left / 2;
                } else {
                  myLength = Math.min(maxSize, left);//如果maxSize比数据块大,则就保留数据块本身大小,如果数据块比maxSize大,则拆分数据块,按照maxSize拆分他成多个数据块
                }
              }
              OneBlockInfo oneblock = new OneBlockInfo(stat.getPath(),
                  myOffset, myLength, locations[i].getHosts(),
                  locations[i].getTopologyPaths());
              left -= myLength;//减少数据块本身大小
              myOffset += myLength;//增加偏移位置

              blocksList.add(oneblock);//添加一个数据块
            } while (left > 0);
          }
          blocks = blocksList.toArray(new OneBlockInfo[blocksList.size()]);
        }
        
        populateBlockInfo(blocks, rackToBlocks, blockToNodes, 
                          nodeToBlocks, rackToNodes);
      }
    }

      /**
       * 添加映射信息
       1.数据块在哪些节点上存在备份
       2.rack上有哪些数据块
       3.一个node节点上有哪些数据块
       4.一个rack上有哪些备份节点集合
       * @param blocks
       * @param rackToBlocks
       * @param blockToNodes
       * @param nodeToBlocks
       * @param rackToNodes
       */
    @VisibleForTesting
    static void populateBlockInfo(OneBlockInfo[] blocks,
                          Map<String, List<OneBlockInfo>> rackToBlocks,
                          Map<OneBlockInfo, String[]> blockToNodes,
                          Map<String, Set<OneBlockInfo>> nodeToBlocks,
                          Map<String, Set<String>> rackToNodes) {
      for (OneBlockInfo oneblock : blocks) {//循环拆分后的每一个数据块
        // add this block to the block --> node locations map
        blockToNodes.put(oneblock, oneblock.hosts);//数据块在哪些节点上存在备份

        // For blocks that do not have host/rack information,
        // assign to default  rack.
        //该数据块所在rack集合
        String[] racks = null;
        if (oneblock.hosts.length == 0) {//没有host,则默认是rack上存在
          racks = new String[]{NetworkTopology.DEFAULT_RACK};
        } else {
          racks = oneblock.racks;
        }

        //映射rack上有哪些数据块
        // add this block to the rack --> block map
        for (int j = 0; j < racks.length; j++) {//循环所有rack集合
          String rack = racks[j];
          List<OneBlockInfo> blklist = rackToBlocks.get(rack);//设置该rack上有哪些数据块
          if (blklist == null) {
            blklist = new ArrayList<OneBlockInfo>();
            rackToBlocks.put(rack, blklist);
          }
          blklist.add(oneblock);
          if (!racks[j].equals(NetworkTopology.DEFAULT_RACK)) {//如果不是默认rack,则添加rack和节点的映射关系,即一个rack上有哪些节点
            // Add this host to rackToNodes map
            addHostToRack(rackToNodes, racks[j], oneblock.hosts[j]);
          }
        }

        //循环该数据块所在的host节点集合
        // add this block to the node --> block map
        for (int j = 0; j < oneblock.hosts.length; j++) {
          String node = oneblock.hosts[j];
          Set<OneBlockInfo> blklist = nodeToBlocks.get(node);//添加一个host节点上有那些数据块
          if (blklist == null) {
            blklist = new LinkedHashSet<OneBlockInfo>();
            nodeToBlocks.put(node, blklist);
          }
          blklist.add(oneblock);
        }
      }
    }

    long getLength() {
      return fileSize;
    }

    OneBlockInfo[] getBlocks() {
      return blocks;
    }
  }

  /**
   * information about one block from the File System
   * 代表一个数据块
   */
  @VisibleForTesting
  static class OneBlockInfo {
    Path onepath;                // name of this file 该文件所在path
    long offset;                 // offset in file
    long length;                 // length of this block
    String[] hosts;              // nodes on which this block resides 该数据块在那些节点上存在
    String[] racks;              // network topology of hosts 一个rack上有多个节点,因此该数据块在哪些rack上存在   命名 host:rack

    OneBlockInfo(Path path, long offset, long len, 
                 String[] hosts, String[] topologyPaths) {
      this.onepath = path;
      this.offset = offset;
      this.hosts = hosts;
      this.length = len;
      assert (hosts.length == topologyPaths.length ||
              topologyPaths.length == 0);

      // if the file system does not have any rack information, then
      // use dummy rack location.
      if (topologyPaths.length == 0) {
        topologyPaths = new String[hosts.length];
        for (int i = 0; i < topologyPaths.length; i++) {
          topologyPaths[i] = (new NodeBase(hosts[i], 
                              NetworkTopology.DEFAULT_RACK)).toString();
        }
      }

      // The topology paths have the host name included as the last 
      // component. Strip it.
      this.racks = new String[topologyPaths.length];
      for (int i = 0; i < topologyPaths.length; i++) {
        this.racks[i] = (new NodeBase(topologyPaths[i])).getNetworkLocation();
      }
    }
  }

  protected BlockLocation[] getFileBlockLocations(
    FileSystem fs, FileStatus stat) throws IOException {
    if (stat instanceof LocatedFileStatus) {
      return ((LocatedFileStatus) stat).getBlockLocations();
    }
    return fs.getFileBlockLocations(stat, 0, stat.getLen());
  }

  private static void addHostToRack(Map<String, Set<String>> rackToNodes,
                                    String rack, String host) {
    Set<String> hosts = rackToNodes.get(rack);
    if (hosts == null) {
      hosts = new HashSet<String>();
      rackToNodes.put(rack, hosts);
    }
    hosts.add(host);
  }
  
  private Set<String> getHosts(Set<String> racks) {
    Set<String> hosts = new HashSet<String>();
    for (String rack : racks) {
      if (rackToNodes.containsKey(rack)) {
        hosts.addAll(rackToNodes.get(rack));
      }
    }
    return hosts;
  }
  
  /**
   * Accept a path only if any one of filters given in the
   * constructor do. 
   */
  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter() {
      this.filters = new ArrayList<PathFilter>();
    }

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public void add(PathFilter one) {
      filters.add(one);
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (filter.accept(path)) {
          return true;
        }
      }
      return false;
    }

    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append("[");
      for (PathFilter f: filters) {
        buf.append(f);
        buf.append(",");
      }
      buf.append("]");
      return buf.toString();
    }
  }
}
