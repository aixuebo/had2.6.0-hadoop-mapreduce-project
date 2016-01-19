package maming.FieldSelectionHelper;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.lib.fieldsel.FieldSelectionHelper;
import org.junit.Test;

public class FieldSelectionHelperTest {

      public static final String DATA_FIELD_SEPERATOR =  "-";
      public static final String MAP_OUTPUT_KEY_VALUE_SPEC = "6,5,1-3:1-";//map的输出
                      

      private List<Integer> mapOutputKeyFieldList = new ArrayList<Integer>();

      private List<Integer> mapOutputValueFieldList = new ArrayList<Integer>();
    @Test
    public void test1(){
        
        String inputData = "0000-0001-0002-0003-0004-0005-0006";
        
        int allMapValueFieldsFrom = FieldSelectionHelper.parseOutputKeyValueSpec(
                MAP_OUTPUT_KEY_VALUE_SPEC, mapOutputKeyFieldList, mapOutputValueFieldList);
        
        System.out.println("allMapValueFieldsFrom:"+allMapValueFieldsFrom);
    }
}
