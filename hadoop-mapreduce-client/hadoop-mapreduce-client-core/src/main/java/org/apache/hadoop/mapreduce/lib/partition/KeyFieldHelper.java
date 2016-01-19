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

package org.apache.hadoop.mapreduce.lib.partition;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.UTF8ByteArrayUtils;

/**
 * This is used in {@link KeyFieldBasedComparator} & 
 * {@link KeyFieldBasedPartitioner}. Defines all the methods
 * for parsing key specifications. The key specification is of the form:
 * -k pos1[,pos2], where pos is of the form f[.c][opts], where f is the number
 *  of the field to use, and c is the number of the first character from the
 *  beginning of the field. Fields and character posns are numbered starting
 *  with 1; a character position of zero in pos2 indicates the field's last
 *  character. If '.c' is omitted from pos1, it defaults to 1 (the beginning
 *  of the field); if omitted from pos2, it defaults to 0 (the end of the
 *  field). opts are ordering options (supported options are 'nr'). 
 *  
 *  例子:
 *  -k2.1,2.98r
 *  格式:
 *  -k+数字(.字符char对应的int),数字(.字符char对应的int)+r+n
 *  其中(.字符char对应的int)是可以省略不写的
 *  
 *  该类的意义是将key按照一定规则拆分,拆分后按照某一部分或者某几个部分进行分区
 */

class KeyFieldHelper {
  
	public static void main(String[] args) {
		KeyFieldHelper keyFieldHelper = new KeyFieldHelper();
	    String option = "-k2,2.98nr";
	    String keyFieldSeparator = "\t";
	    keyFieldHelper.setKeyFieldSeparator(keyFieldSeparator);
	    keyFieldHelper.parseOption(option);
	    
	    String text  = "11 22 3";
	    keyFieldHelper.getWordLengths(text.getBytes(), 0, text.length());
	}
  protected static class KeyDescription {
    int beginFieldIdx = 1;//第几段开始
    int beginChar = 1;//第beginFieldIdx段的第几个元素开始
    int endFieldIdx = 0;//第几段结束
    int endChar = 0;//第endFieldIdx段的第几个元素结束
    boolean numeric;
    boolean reverse;
    @Override
    public String toString() {
      return "-k" 
             + beginFieldIdx + "." + beginChar + "," 
             + endFieldIdx + "." + endChar 
             + (numeric ? "n" : "") + (reverse ? "r" : "");
    }
  }
  
  private List<KeyDescription> allKeySpecs = new ArrayList<KeyDescription>();
  private byte[] keyFieldSeparator;///分隔符
  private boolean keySpecSeen = false;
  
  //设置分隔符
  public void setKeyFieldSeparator(String keyFieldSeparator) {
    try {
      this.keyFieldSeparator =
        keyFieldSeparator.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("The current system does not " +
          "support UTF-8 encoding!", e);
    }    
  }
  
  /** Required for backcompatibility with num.key.fields.for.partition in
   * {@link KeyFieldBasedPartitioner} */
  public void setKeyFieldSpec(int start, int end) {
    if (end >= start) {
      KeyDescription k = new KeyDescription();
      k.beginFieldIdx = start;
      k.endFieldIdx = end;
      keySpecSeen = true;
      allKeySpecs.add(k);
    }
  }
  
  public List<KeyDescription> keySpecs() {
    return allKeySpecs;
  }
    
  /**
   * 对字符串进行按照分隔符拆分
   * 比如hello how are you 按照空格拆分为5 3 3 3,一共拆分成4组
   * 因此最终结果为4,5, 3, 3, 3
   */
  public int[] getWordLengths(byte []b, int start, int end) {
    //Given a string like "hello how are you", it returns an array
    //like [4 5, 3, 3, 3], where the first element is the number of
	//fields
    if (!keySpecSeen) {
      //if there were no key specs, then the whole key is one word
      return new int[] {1};
    }
    int[] lengths = new int[10];
    int currLenLengths = lengths.length;
    int idx = 1;
    int pos;
    while ((pos = UTF8ByteArrayUtils.findBytes(b, start, end, 
        keyFieldSeparator)) != -1) {//进入该循环,说明该行文件有分隔符keyFieldSeparator
      if (++idx == currLenLengths) {//扩容
        int[] temp = lengths;
        lengths = new int[(currLenLengths = currLenLengths*2)];
        System.arraycopy(temp, 0, lengths, 0, temp.length);
      }
      lengths[idx - 1] = pos - start;//数组的位置存储每一个分隔符分割了多少个字符
      start = pos + 1;
    }
    
    if (start != end) {
      lengths[idx] = end - start;
    }
    lengths[0] = idx; //number of words is the first element 第0个位置设置有多少组
    return lengths;
  }
  
  /**
   *  符合的字符串的第一个位置
   */
  public int getStartOffset(byte[]b, int start, int end, 
      int []lengthIndices, KeyDescription k) {
    //if -k2.5,2 is the keyspec, the startChar is lengthIndices[1] + 5
    //note that the [0]'th element is the number of fields in the key
    if (lengthIndices[0] >= k.beginFieldIdx) {//说明分隔符在其中,因此可以查找分隔符
      int position = 0;
      for (int i = 1; i < k.beginFieldIdx; i++) {
        position += lengthIndices[i] + keyFieldSeparator.length; 
      }
      if (position + k.beginChar <= (end - start)) {
        return start + position + k.beginChar - 1; 
      }
    }
    return -1;
  }
  public int getEndOffset(byte[]b, int start, int end, 
      int []lengthIndices, KeyDescription k) {
    //if -k2,2.8 is the keyspec, the endChar is lengthIndices[1] + 8
    //note that the [0]'th element is the number of fields in the key
    if (k.endFieldIdx == 0) {//没有设置end,则说明没有设置-k2之后逗号以后的数据,因此说明默认一直到结尾,因此是end-l,即最后一个位置
      //there is no end field specified for this keyspec. So the remaining
      //part of the key is considered in its entirety.
      return end - 1; 
    }
    if (lengthIndices[0] >= k.endFieldIdx) {//超出最大split段数了,则说明一直到最后的意思,因此直接走else,返回end-l,即最后一个位置
      int position = 0;
      int i;
      for (i = 1; i < k.endFieldIdx; i++) {
        position += lengthIndices[i] + keyFieldSeparator.length;
      }
      if (k.endChar == 0) { 
        position += lengthIndices[i];
      }
      if (position + k.endChar <= (end - start)) {
        return start + position + k.endChar - 1;
      }
      return end - 1;
    }
    return end - 1;
  }
  public void parseOption(String option) {
    if (option == null || option.equals("")) {
      //we will have only default comparison
      return;
    }
    /**
     *  option按照\t\n\r\f拆分
     */
    StringTokenizer args = new StringTokenizer(option);
    KeyDescription global = new KeyDescription();//全局默认值
    while (args.hasMoreTokens()) {
      String arg = args.nextToken();
      if (arg.equals("-n")) {  
        global.numeric = true;
      }
      if (arg.equals("-r")) {
        global.reverse = true;
      }
      if (arg.equals("-nr")) {
        global.numeric = true;
        global.reverse = true;
      }
      if (arg.startsWith("-k")) {
        KeyDescription k = parseKey(arg, args);
        if (k != null) {
          allKeySpecs.add(k);
          keySpecSeen = true;
        }
      }
    }
    for (KeyDescription key : allKeySpecs) {
    	System.out.println("1:"+key);
      if (!(key.reverse | key.numeric)) {//当既没有设置reverse -r,也没有设置-n的时候,要被设置成全局的默认值
        key.reverse = global.reverse;
        key.numeric = global.numeric;
      }
  	System.out.println("2:"+key);
    }
    if (allKeySpecs.size() == 0) {
      allKeySpecs.add(global);
    }
  }
  
  private KeyDescription parseKey(String arg, StringTokenizer args) {
    //we allow for -k<arg> and -k <arg>
    String keyArgs = null;
    if (arg.length() == 2) {
      if (args.hasMoreTokens()) {
        keyArgs = args.nextToken();
      }
    } else {//去除-k
      keyArgs = arg.substring(2);
    }
    if (keyArgs == null || keyArgs.length() == 0) {
      return null;
    }
    StringTokenizer st = new StringTokenizer(keyArgs,"nr.,",true);
       
    KeyDescription key = new KeyDescription();
    
    String token;
    //the key is of the form 1[.3][nr][,1.5][nr]
    if (st.hasMoreTokens()) {
      token = st.nextToken();
      //the first token must be a number
      key.beginFieldIdx = Integer.parseInt(token);
    }
    if (st.hasMoreTokens()) {
      token = st.nextToken();
      if (token.equals(".")) {
        token = st.nextToken();
        key.beginChar = Integer.parseInt(token);
        if (st.hasMoreTokens()) {
          token = st.nextToken();
        } else {
          return key;
        }
      } 
      do {
        if (token.equals("n")) {
          key.numeric = true;
        }
        else if (token.equals("r")) {
          key.reverse = true;
        }
        else break;
        if (st.hasMoreTokens()) {
          token = st.nextToken();
        } else {
          return key;
        }
      } while (true);
      
      if (token.equals(",")) {
        token = st.nextToken();
        //the first token must be a number
        key.endFieldIdx = Integer.parseInt(token);
        if (st.hasMoreTokens()) {
          token = st.nextToken();
          if (token.equals(".")) {
            token = st.nextToken();
            key.endChar = Integer.parseInt(token);
            if (st.hasMoreTokens()) {
              token = st.nextToken();
            } else {
              return key;
            }
          }
          do {
            if (token.equals("n")) {
              key.numeric = true;
            }
            else if (token.equals("r")) {
              key.reverse = true;
            }
            else { 
              throw new IllegalArgumentException("Invalid -k argument. " +
               "Must be of the form -k pos1,[pos2], where pos is of the form " +
               "f[.c]nr");
            }
            if (st.hasMoreTokens()) {
              token = st.nextToken();
            } else {
              break;
            }
          } while (true);
        }
        return key;
      }
      throw new IllegalArgumentException("Invalid -k argument. " +
          "Must be of the form -k pos1,[pos2], where pos is of the form " +
          "f[.c]nr");
    }
    return key;
  }
  private void printKey(KeyDescription key) {
    System.out.println("key.beginFieldIdx: " + key.beginFieldIdx);
    System.out.println("key.beginChar: " + key.beginChar);
    System.out.println("key.endFieldIdx: " + key.endFieldIdx);
    System.out.println("key.endChar: " + key.endChar);
    System.out.println("key.numeric: " + key.numeric);
    System.out.println("key.reverse: " + key.reverse);
    System.out.println("parseKey over");
  }  
}
