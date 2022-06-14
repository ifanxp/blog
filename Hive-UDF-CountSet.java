package cn.com.iresearch.ifan.bitwise;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

/*
用法：
add jar /home/irsuser/ifan/hive/Hive-UDAF-test.jar;
create temporary function my_countset as 'cn.com.iresearch.ifan.bitwise.CountSet';
select my_countset(3);
*/

public class CountSet extends UDF {
    public int evaluate(int i) {
        int n = i; //i.get();
        n = n - ( ( n >> 1 ) & 1431655765 );
        n = ( n &  858993459 ) + ( ( n >> 2 ) &  858993459 );
        n = ( n &  252645135 ) + ( ( n >> 4 ) &  252645135 );
        n = n + ( n >> 8 );
        n = n + ( n >> 16 );
        n = n & 63;
        return n;
        // return new IntWritable(n);
    }
}
