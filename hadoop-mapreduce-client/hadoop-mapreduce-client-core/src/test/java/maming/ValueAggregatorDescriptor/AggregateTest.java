package maming.ValueAggregatorDescriptor;

import java.text.NumberFormat;

import org.junit.Test;

public class AggregateTest {

  private static NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setMinimumIntegerDigits(4);
    idFormat.setGroupingUsed(false);
  }
  
  /**
inputData:
0001
0002 0002
0003 0003 0003
0004 0004 0004 0004
0005 0005 0005 0005 0005
0006 0006 0006 0006 0006 0006
0007 0007 0007 0007 0007 0007 0007
0008 0008 0008 0008 0008 0008 0008 0008
0009 0009 0009 0009 0009 0009 0009 0009 0009
0010 0010 0010 0010 0010 0010 0010 0010 0010 0010
0011 0011 0011 0011 0011 0011 0011 0011 0011 0011 0011
0012 0012 0012 0012 0012 0012 0012 0012 0012 0012 0012 0012
0013 0013 0013 0013 0013 0013 0013 0013 0013 0013 0013 0013 0013
0014 0014 0014 0014 0014 0014 0014 0014 0014 0014 0014 0014 0014 0014
0015 0015 0015 0015 0015 0015 0015 0015 0015 0015 0015 0015 0015 0015 0015
0016 0016 0016 0016 0016 0016 0016 0016 0016 0016 0016 0016 0016 0016 0016 0016
0017 0017 0017 0017 0017 0017 0017 0017 0017 0017 0017 0017 0017 0017 0017 0017 0017
0018 0018 0018 0018 0018 0018 0018 0018 0018 0018 0018 0018 0018 0018 0018 0018 0018 0018
0019 0019 0019 0019 0019 0019 0019 0019 0019 0019 0019 0019 0019 0019 0019 0019 0019 0019 0019

expectedOutput:
max 19
min 1
count_0001  1
count_0002  2
count_0003  3
count_0004  4
count_0005  5
count_0006  6
count_0007  7
count_0008  8
count_0009  9
count_0010  10
count_0011  11
count_0012  12
count_0013  13
count_0014  14
count_0015  15
count_0016  16
count_0017  17
count_0018  18
count_0019  19
value_as_string_max 9
value_as_string_min 1
uniq_count  15

   */
  @Test
  public void test1(){
    
    int numOfInputLines = 20;
    
    StringBuffer inputData = new StringBuffer();//输出
    StringBuffer expectedOutput = new StringBuffer();//期望得到的输出
    expectedOutput.append("max\t19\n");
    expectedOutput.append("min\t1\n"); 
    
    for (int i = 1; i < numOfInputLines; i++) {
      expectedOutput.append("count_").append(idFormat.format(i));
      expectedOutput.append("\t").append(i).append("\n");

      inputData.append(idFormat.format(i));
      for (int j = 1; j < i; j++) {
        inputData.append(" ").append(idFormat.format(i));
      }
      inputData.append("\n");
    }
    expectedOutput.append("value_as_string_max\t9\n");
    expectedOutput.append("value_as_string_min\t1\n");
    expectedOutput.append("uniq_count\t15\n");
    
    
    System.out.println("inputData:");
    System.out.println(inputData.toString());
    System.out.println("expectedOutput:");
    System.out.println(expectedOutput.toString());
    
  }
  
}
