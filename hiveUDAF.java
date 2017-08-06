package hiveudfdemo;

/*
 * http://blog.csdn.net/moxuqiang_dm/article/details/47401063
 * http://www.cnblogs.com/ggjucheng/archive/2013/02/01/2888051.html
 * http://blog.csdn.net/kent7306/article/details/50110067 
 */

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

public class AggOr implements GenericUDAFResolver2 {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        return new udafBitOrEvaluators();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo) throws SemanticException {
        ObjectInspector[] parameters = paramInfo.getParameterObjectInspectors();
        assert (parameters.length == 1);
        return new udafBitOrEvaluators();
    }

    public static class udafBitOrEvaluators extends GenericUDAFEvaluator {
        private IntObjectInspector partialResult;
        private IntWritable result;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);
            partialResult = (IntObjectInspector) parameters[0];
            result = new IntWritable(0);
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        }

        @AggregationType(estimable = true)
        static class BitOrAgg extends AbstractAggregationBuffer {
            int value;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            BitOrAgg buffer = new BitOrAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((BitOrAgg) agg).value = 0;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            merge(agg, parameters[0]);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null && partialResult != null) {
                int p = partialResult.get(partial);
                ((BitOrAgg) agg).value |= p;
            }
        }

        // 最终被调用返回结果；
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            result.set(((BitOrAgg) agg).value);
            return result;
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }
    }

}
