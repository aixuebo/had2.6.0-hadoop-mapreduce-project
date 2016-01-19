package maming.partation;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;

public class KeyFieldBasedComparatorTest {

  @Test
  public void test1(){
    try {
      
      File fileOut = new File("E://tmp/out.log");
      DataOutput dataOutput = new DataOutputStream(new FileOutputStream(fileOut));
      String message = "abcdef";
      WritableUtils.writeCompressedString(dataOutput, message);
      
      DataInput dataInput = new DataInputStream(new FileInputStream(fileOut));
      message = WritableUtils.readCompressedString(dataInput);
      System.out.println(message);
      System.out.println(WritableUtils.decodeVIntSize(message.getBytes()[0]));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
