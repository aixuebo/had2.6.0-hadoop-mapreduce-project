package maming.partation;

import org.junit.Test;

public class IntervalSamplerTest {

  @Test
  public void test1(){
    long records = 0;
    long kept = 0;
    double freq = 0.3;
    
    int count = 0;
    
    for(int i=0;i<100;i++) {
      ++records;
      System.out.println((double) kept / records);
      if ((double) kept / records < freq) {
        System.out.println("----"+i);
        ++kept;
        count++;
      }
    }
    
    System.out.println("count:"+count);
    
  }
}
