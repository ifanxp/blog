package test;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
@SuppressWarnings("deprecation")
public class BitOr extends UDAF {
    // Define Logging
    static final Log LOG = LogFactory.getLog(BitOr.class.getName());
    public static class BitOrUDAFEvaluator implements UDAFEvaluator {
        public static class Column {
            int sum = 0;
        }
        private Column col = null;
        public BitOrUDAFEvaluator() {
            super();
            init();
        }

        public void init() {
            LOG.debug("Initialize evaluator");
            col = new Column();
        }

        public boolean iterate(int value) throws HiveException {
            LOG.debug("Iterating over each value for aggregation");
            if (col == null)
                throw new HiveException("Item is not initialized");
            col.sum |= value;
            return true;
        }
        // C - Called when Hive wants partially aggregated results.
        public Column terminatePartial() {
            LOG.debug("Return partially aggregated results");
            return col;
        }
        // D - Called when Hive decides to combine one partial aggregation with
        // another

        public boolean merge(Column other) {
            LOG.debug("merging by combining partial aggregation");
            if (other == null) {
                return true;
            }
            col.sum |= other.sum;
            return true;
        }
        // E - Called when the final result of the aggregation needed.
        public int terminate() {
            LOG.debug("At the end of last record of the group - returning final result");
            return col.sum;
        }
    }
}
