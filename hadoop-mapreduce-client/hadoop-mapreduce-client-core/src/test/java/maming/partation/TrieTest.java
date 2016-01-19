package maming.partation;

import java.util.Arrays;

/**
 * trie树测试 BinaryComparable
 */
public class TrieTest<T> {

  public String[] keySplit = new String[]{"abc","def","ghi","klm","xyz"};
  public int maxDepth = 50;
  
  public static void main(String[] args) {
    TrieTest<String> test = new TrieTest<String>();
    String[] keySplit = test.keySplit;
    int maxDepth = test.maxDepth;
    System.out.println(test.buildTrie(keySplit, 0, keySplit.length, new byte[0], maxDepth));
  }
  /**
   * Interface to the partitioner to locate a key in the partition keyset.
   */
  interface Node<T> {
    /**
     * Locate partition in keyset K, st [Ki..Ki+1) defines a partition,
     * with implicit K0 = -inf, Kn = +inf, and |K| = #partitions - 1.
     * 为key寻找他是属于第几个reduce
     */
    int findPartition(T key);
  }

  /**
   * Base class for trie nodes. If the keytype is memcomp-able, this builds
   * tries of the first <tt>total.order.partitioner.max.trie.depth</tt>
   * bytes.
   * 树的层次
   */
  static abstract class TrieNode implements Node<String> {
    private final int level;
    TrieNode(int level) {
      this.level = level;
    }
    int getLevel() {
      return level;
    }
  }
  

  /**
   * An inner trie node that contains 256 children based on the next
   * character.
   */
  class InnerTrieNode extends TrieNode {
    private TrieNode[] child = new TrieNode[256];

    InnerTrieNode(int level) {
      super(level);
    }
    public int findPartition(String key) {
      int level = getLevel();
      if (key.length() <= level) {
        return child[0].findPartition(key);
      }
      return child[0xFF & key.getBytes()[level]].findPartition(key);
    }
    
    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("lever:").append(this.getLevel()).append("\t").append(this.child.length);
      for(TrieNode node:this.child){
        sb.append("\t").append(node);
      }
      return sb.toString();
    }
  }
  
  /**
   * @param level        the tree depth at this node
   * @param splitPoints  the full split point vector, which holds
   *                     the split point or points this leaf node
   *                     should contain
   * @param lower        first INcluded element of splitPoints
   * @param upper        first EXcluded element of splitPoints
   * @return  a leaf node.  They come in three kinds: no split points 
   *          [and the findParttion returns a canned index], one split
   *          point [and we compare with a single comparand], or more
   *          than one [and we do a binary search].  The last case is
   *          rare.
   */
  private TrieNode LeafTrieNodeFactory
             (int level, String[] splitPoints, int lower, int upper) {
      switch (upper - lower) {
      case 0:
          return new UnsplitTrieNode(level, lower);
          
      case 1:
          return new SinglySplitTrieNode(level, splitPoints, lower);
          
      default:
          return new LeafTrieNode(level, splitPoints, lower, upper);
      }
  }

  /**
   * A leaf trie node that scans for the key between lower..upper.
   * 
   * We don't generate many of these now, since we usually continue trie-ing 
   * when more than one split point remains at this level. and we make different
   * objects for nodes with 0 or 1 split point.
   */
  private class LeafTrieNode extends TrieNode {
    final int lower;
    final int upper;
    final String[] splitPoints;
    LeafTrieNode(int level, String[] splitPoints, int lower, int upper) {
      super(level);
      this.lower = lower;
      this.upper = upper;
      this.splitPoints = splitPoints;
    }
    public int findPartition(String key) {
      final int pos = Arrays.binarySearch(splitPoints, lower, upper, key) + 1;
      return (pos < 0) ? -pos : pos;
    }
    
    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("LeafTrieNode:").append(lower).append("\t").append(upper);
      for(String node:this.splitPoints){
        sb.append("\t").append(node);
      }
      return sb.toString();
    }
  }
  
  private class UnsplitTrieNode extends TrieNode {
      final int result;
      
      UnsplitTrieNode(int level, int value) {
          super(level);
          this.result = value;
      }
      
      public int findPartition(String key) {
          return result;
      }
      
      @Override
      public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("UnsplitTrieNode:").append(result);
        return sb.toString();
      }
  }
  
  private class SinglySplitTrieNode extends TrieNode {
      final int               lower;
      final String  mySplitPoint;
      
      SinglySplitTrieNode(int level, String[] splitPoints, int lower) {
          super(level);
          this.lower = lower;
          this.mySplitPoint = splitPoints[lower];
      }
      
      public int findPartition(String key) {
          return lower + (key.compareTo(mySplitPoint) < 0 ? 0 : 1);
      }
      
      
      @Override
      public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("SinglySplitTrieNode:").append(lower).append("\t").append(mySplitPoint);
        return sb.toString();
      }
  }
  
  /**
   * 
   * This object contains a TrieNodeRef if there is such a thing that
   * can be repeated.  Two adjacent trie node slots that contain no 
   * split points can be filled with the same trie node, even if they
   * are not on the same level.  See buildTreeRec, below.
   *
   */  
  private class CarriedTrieNodeRef
  {
      TrieNode   content;
      
      CarriedTrieNodeRef() {
          content = null;
      }
  }

  
  /**
   * Given a sorted set of cut points, build a trie that will find the correct
   * partition quickly.
   * @param splits the list of cut points拆分后的集合
   * @param lower the lower bound of partitions 0..numPartitions-1
   * @param upper the upper bound of partitions 0..numPartitions-1
   * @pa
   * @param maxDepth the maximum depth we will build ram prefix the prefix that we have already checked againsta trie for 树的最大的深度
   * @return the trie node that will divide the splits correctly
   * 构建自然排序trie树
   */
  private TrieNode buildTrie(String[] splits, int lower,
          int upper, byte[] prefix, int maxDepth) {
      return buildTrieRec
               (splits, lower, upper, prefix, maxDepth, new CarriedTrieNodeRef());
  }
  
  /**
   * This is the core of buildTrie.  The interface, and stub, above, just adds
   * an empty CarriedTrieNodeRef.  
   * 
   * We build trie nodes in depth first order, which is also in key space
   * order.  Every leaf node is referenced as a slot in a parent internal
   * node.  If two adjacent slots [in the DFO] hold leaf nodes that have
   * no split point, then they are not separated by a split point either, 
   * because there's no place in key space for that split point to exist.
   * 
   * When that happens, the leaf nodes would be semantically identical, and
   * we reuse the object.  A single CarriedTrieNodeRef "ref" lives for the 
   * duration of the tree-walk.  ref carries a potentially reusable, unsplit
   * leaf node for such reuse until a leaf node with a split arises, which 
   * breaks the chain until we need to make a new unsplit leaf node.
   * 
   * Note that this use of CarriedTrieNodeRef means that for internal nodes, 
   * for internal nodes if this code is modified in any way we still need 
   * to make or fill in the subnodes in key space order.
   */
  private TrieNode buildTrieRec(String[] splits, int lower,
      int upper, byte[] prefix, int maxDepth, CarriedTrieNodeRef ref) {
    final int depth = prefix.length;
    // We generate leaves for a single split point as well as for 
    // no split points.
    if (depth >= maxDepth || lower >= upper - 1) {
        // If we have two consecutive requests for an unsplit trie node, we
        // can deliver the same one the second time.
        if (lower == upper && ref.content != null) {
            return ref.content;
        }
        TrieNode  result = LeafTrieNodeFactory(depth, splits, lower, upper);
        ref.content = lower == upper ? result : null;
        return result;
    }
    InnerTrieNode result = new InnerTrieNode(depth);
    byte[] trial = Arrays.copyOf(prefix, prefix.length + 1);
    // append an extra byte on to the prefix
    int         currentBound = lower;
    for(int ch = 0; ch < 0xFF; ++ch) {//0xFF = 255
      trial[depth] = (byte) (ch + 1);
      lower = currentBound;
      while (currentBound < upper) {
        if (splits[currentBound].compareTo(new String(trial, 0, trial.length)) >= 0) {
          break;
        }
        currentBound += 1;
      }
      trial[depth] = (byte) ch;
      result.child[0xFF & ch]
                   = buildTrieRec(splits, lower, currentBound, trial, maxDepth, ref);
    }
    // pick up the rest
    trial[depth] = (byte)0xFF;
    result.child[0xFF] 
                 = buildTrieRec(splits, lower, currentBound, trial, maxDepth, ref);
    
    return result;
  }
  
}
