package cn.com.iresearch.ifan.bitwise;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;
import java.util.ArrayList;

/* 
参考：
https://blog.csdn.net/a805814077/article/details/104645447

用法：
add jar /home/irsuser/ifan/hive/Hive-UDAF-test.jar;
create temporary function my_bit_or as 'cn.com.iresearch.ifan.bitwise.BitOR';
select my_bit_or(num) as nums from (select 1 as num union all select 2 as num) as t;
*/

public class BitOR extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(BitOR.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 1) {
            throw new UDFArgumentLengthException("输入参数个数非法，一个参数");
        }

        switch (((PrimitiveTypeInfo) info[0]).getPrimitiveCategory()) {
            case SHORT:
            case INT:
            case LONG:
                return new GenericEvaluate();
            default:
                throw new UDFArgumentLengthException("仅支持smallint/int/bigint");
        }
    }

    public static class GenericEvaluate extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector input;

        private StructObjectInspector soi;
        private StructField countField;
        private StructField resultFeild;
        private LongObjectInspector countFeildOI;
        private LongObjectInspector resultFeildOI;

        private Object[] partialResult;
        private LongWritable result ;                   //保存最终结果
        private MyAggregationBuffer myAggregationBuffer;  //自定义聚合列，保存临时结果

        //自定义AggregationBuffer
        public static class MyAggregationBuffer implements AggregationBuffer {
            long count;
            long result;
        }

        @Override  //指定返回类型
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                input = (PrimitiveObjectInspector) parameters[0];
            }
            else {
                soi = (StructObjectInspector) parameters[0];
                countField = soi.getStructFieldRef("count");
                resultFeild = soi.getStructFieldRef("result");
                countFeildOI = (LongObjectInspector) countField.getFieldObjectInspector();
                resultFeildOI = (LongObjectInspector) resultFeild.getFieldObjectInspector();
            }

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                partialResult = new Object[2];
                partialResult[0] = new LongWritable(0);
                partialResult[1] = new LongWritable(0);
                ArrayList<String> fname = new ArrayList<String>();
                fname.add("count");
                fname.add("result");
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
            }
            else {
                result = new LongWritable(0);
                return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            }
        }

        @Override   //获得一个聚合的缓冲对象，每个map执行一次
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MyAggregationBuffer myAggregationBuffer = new MyAggregationBuffer();
            reset(myAggregationBuffer);  // 重置聚合值
            return myAggregationBuffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            MyAggregationBuffer newAgg = (MyAggregationBuffer) agg;
            newAgg.result = 0L;
            newAgg.count = 0L;
        }

        boolean warned = false;

        @Override  // 传入参数值聚合
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] == null) return;
            MyAggregationBuffer myAgg = (MyAggregationBuffer) agg;
            try {
                long inputNum = PrimitiveObjectInspectorUtils.getLong(parameters[0], input);
                myAgg.result |= inputNum;
                myAgg.count ++;
            } catch (NumberFormatException e) {
                warned = true;
                LOG.warn(getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
                LOG.warn(getClass().getSimpleName()+" ignoring similar exceptions.");
            }
        }

        @Override  // iterate 输出中间结果
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MyAggregationBuffer newAgg = (MyAggregationBuffer) agg;
            //result.set(newAgg.result);
            ((LongWritable) partialResult[0]).set(newAgg.count);
            ((LongWritable) partialResult[1]).set(newAgg.result);
            return partialResult;
        }

        @Override  // 合并
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) return;
            MyAggregationBuffer newAgg = (MyAggregationBuffer) agg;
            Object partialCount = soi.getStructFieldData(partial, countField);
            Object partialResult = soi.getStructFieldData(partial, resultFeild);
            newAgg.count += countFeildOI.get(partialCount);
            newAgg.result |= resultFeildOI.get(partialResult);
        }

        @Override  //输出最终结果
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MyAggregationBuffer myAgg = (MyAggregationBuffer) agg;
            if (myAgg.count == 0) return -1;
            result.set(myAgg.result);
            return result;
        }
    }
}
