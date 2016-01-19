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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class TestMRKeyFieldBasedPartitioner extends TestCase {

	public void test1(){
		 int numReducers = 10;
		 
		    System.out.println("method 111111111 设置num.key.fields.for.partition");
		    KeyFieldBasedPartitioner<Text, Text> kfbp = 
		      new KeyFieldBasedPartitioner<Text, Text>();
		    Configuration conf = new Configuration();
		    conf.setInt("num.key.fields.for.partition", 10);
		    kfbp.setConf(conf);
		    System.out.println(kfbp.getPartition(new Text(), new Text(), numReducers));
		    
		    System.out.println("method 2222222222222 没有KeyDescription");
			  // check if the hashcode is correct when no keyspec is specified
		    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
		    conf = new Configuration();
		    kfbp.setConf(conf);
		    String input = "abc\tdef\txyz";
		    int hashCode = input.hashCode();
		    int expectedPartition = kfbp.getPartition(hashCode, numReducers);
		 System.out.println(kfbp.getPartition(new Text(input), new Text(), numReducers));
		 System.out.println("expectedPartition:"+expectedPartition);
		 
		    System.out.println("method 33333 没有KeyDescription");
		    // check if the hashcode is correct with specified keyspec
		    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
		    conf = new Configuration();
		    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2,2");
		    kfbp.setConf(conf);
		    String expectedOutput = "def";
		    byte[] eBytes = expectedOutput.getBytes();
		    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
		    expectedPartition = kfbp.getPartition(hashCode, numReducers);
		    System.out.println(kfbp.getPartition(new Text(input), new Text(), numReducers));//abc\tdef\txyz
			 System.out.println("expectedPartition:"+expectedPartition);
		 
		  System.out.println("method 4444 测试最后无效的end index,即end的结果超过了总大小");
		// test with invalid end index in keyspecs
		    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
		    conf = new Configuration();
		    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2,5");//一共就三段,他end要第5段，那是不可能得到的
		    kfbp.setConf(conf);
		    expectedOutput = "def\txyz";
		    eBytes = expectedOutput.getBytes();
		    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
		    expectedPartition = kfbp.getPartition(hashCode, numReducers);
		    
		    System.out.println(expectedPartition);
		    System.out.println(kfbp.getPartition(new Text(input), new Text(), numReducers));
		    
		    System.out.println("method 5555 测试没有end index");
		    // test with 0 end index in keyspecs
		    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
		    conf = new Configuration();
		    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2");//没有设置,因此没有end index,说明默认end index = 0
		    kfbp.setConf(conf);
		    expectedOutput = "def\txyz";
		    eBytes = expectedOutput.getBytes();
		    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
		    expectedPartition = kfbp.getPartition(hashCode, numReducers);
		    
		    System.out.println(expectedPartition);
		    System.out.println(kfbp.getPartition(new Text(input), new Text(), numReducers));
		    
		    System.out.println("method 6666 测试非法数据,返回结果是0");
		    // test with invalid keyspecs
		    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
		    conf = new Configuration();
		    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k10");
		    kfbp.setConf(conf);
		    System.out.println(kfbp.getPartition(new Text(input), new Text(), numReducers));//结果应该等于0
		    
		    System.out.println("method 7777 测试多个段数据");
		    // test with multiple keyspecs
		    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
		    conf = new Configuration();
		    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2,2 -k4,4");
		    kfbp.setConf(conf);
		    input = "abc\tdef\tpqr\txyz";
		    expectedOutput = "def";
		    eBytes = expectedOutput.getBytes();
		    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
		    expectedOutput = "xyz";
		    eBytes = expectedOutput.getBytes();
		    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, hashCode);//其中的hashCode是def的结果集
		    expectedPartition = kfbp.getPartition(hashCode, numReducers);
		    
		    System.out.println(expectedPartition);//结果是def+xyz共同做partition
		    System.out.println(kfbp.getPartition(new Text(input), new Text(), numReducers));
		    
		    System.out.println("method 8888 测试非法的start index");
		    // test with invalid start index in keyspecs
		    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
		    conf = new Configuration();
		    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2,2 -k30,21 -k4,4 -k5");//因为-k30 -k5都超过了整体四个段,因此他们start部分会返回-1,因此会被执行continue操作,相当于没有设置这两个选项
		    kfbp.setConf(conf);
		    expectedOutput = "def";
		    eBytes = expectedOutput.getBytes();
		    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
		    expectedOutput = "xyz";
		    eBytes = expectedOutput.getBytes();
		    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, hashCode);
		    expectedPartition = kfbp.getPartition(hashCode, numReducers);
		    
		    System.out.println(expectedPartition);//结果是def+xyz共同做partition
		    System.out.println(kfbp.getPartition(new Text(input), new Text(), numReducers));
	}
	
	
	public static void main(String[] args) {
		TestMRKeyFieldBasedPartitioner test = new TestMRKeyFieldBasedPartitioner();
		test.test1();
				
	}
  /**
   * Test is key-field-based partitioned works with empty key.
   */
  public void testEmptyKey() throws Exception {
    int numReducers = 10;
    KeyFieldBasedPartitioner<Text, Text> kfbp = 
      new KeyFieldBasedPartitioner<Text, Text>();
    Configuration conf = new Configuration();
    conf.setInt("num.key.fields.for.partition", 10);
    kfbp.setConf(conf);
    assertEquals("Empty key should map to 0th partition", 
                 0, kfbp.getPartition(new Text(), new Text(), numReducers));
    
    // check if the hashcode is correct when no keyspec is specified
    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
    conf = new Configuration();
    kfbp.setConf(conf);
    String input = "abc\tdef\txyz";
    int hashCode = input.hashCode();
    int expectedPartition = kfbp.getPartition(hashCode, numReducers);
    assertEquals("Partitioner doesnt work as expected", expectedPartition, 
                 kfbp.getPartition(new Text(input), new Text(), numReducers));
    
    // check if the hashcode is correct with specified keyspec
    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
    conf = new Configuration();
    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2,2");
    kfbp.setConf(conf);
    String expectedOutput = "def";
    byte[] eBytes = expectedOutput.getBytes();
    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
    expectedPartition = kfbp.getPartition(hashCode, numReducers);
    assertEquals("Partitioner doesnt work as expected", expectedPartition, 
                 kfbp.getPartition(new Text(input), new Text(), numReducers));
    
    // test with invalid end index in keyspecs
    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
    conf = new Configuration();
    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2,5");
    kfbp.setConf(conf);
    expectedOutput = "def\txyz";
    eBytes = expectedOutput.getBytes();
    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
    expectedPartition = kfbp.getPartition(hashCode, numReducers);
    assertEquals("Partitioner doesnt work as expected", expectedPartition, 
                 kfbp.getPartition(new Text(input), new Text(), numReducers));
    
    // test with 0 end index in keyspecs
    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
    conf = new Configuration();
    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2");
    kfbp.setConf(conf);
    expectedOutput = "def\txyz";
    eBytes = expectedOutput.getBytes();
    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
    expectedPartition = kfbp.getPartition(hashCode, numReducers);
    assertEquals("Partitioner doesnt work as expected", expectedPartition, 
                 kfbp.getPartition(new Text(input), new Text(), numReducers));
    
    // test with invalid keyspecs
    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
    conf = new Configuration();
    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k10");
    kfbp.setConf(conf);
    assertEquals("Partitioner doesnt work as expected", 0, 
                 kfbp.getPartition(new Text(input), new Text(), numReducers));
    
    // test with multiple keyspecs
    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
    conf = new Configuration();
    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2,2 -k4,4");
    kfbp.setConf(conf);
    input = "abc\tdef\tpqr\txyz";
    expectedOutput = "def";
    eBytes = expectedOutput.getBytes();
    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
    expectedOutput = "xyz";
    eBytes = expectedOutput.getBytes();
    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, hashCode);
    expectedPartition = kfbp.getPartition(hashCode, numReducers);
    assertEquals("Partitioner doesnt work as expected", expectedPartition, 
                 kfbp.getPartition(new Text(input), new Text(), numReducers));
    
    // test with invalid start index in keyspecs
    kfbp = new KeyFieldBasedPartitioner<Text, Text>();
    conf = new Configuration();
    conf.set(KeyFieldBasedPartitioner.PARTITIONER_OPTIONS, "-k2,2 -k30,21 -k4,4 -k5");
    kfbp.setConf(conf);
    expectedOutput = "def";
    eBytes = expectedOutput.getBytes();
    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, 0);
    expectedOutput = "xyz";
    eBytes = expectedOutput.getBytes();
    hashCode = kfbp.hashCode(eBytes, 0, eBytes.length - 1, hashCode);
    expectedPartition = kfbp.getPartition(hashCode, numReducers);
    assertEquals("Partitioner doesnt work as expected", expectedPartition, 
                 kfbp.getPartition(new Text(input), new Text(), numReducers));
  }
}
