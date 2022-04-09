\import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
 
class IntegerBitOr extends UserDefinedAggregateFunction{
    override def inputSchema:StructType = StructType(Array(StructField("value",IntegerType)))
    override def bufferSchema:StructType = StructType(Array(StructField("result",IntegerType)))
    override def dataType:DataType = IntegerType
    override def deterministic:Boolean = true
    override def initialize(buffer:MutableAggregationBuffer):Unit = {buffer(0)=0}
    
    override def update(buffer:MutableAggregationBuffer,input:Row):Unit={
        buffer(0) = buffer.getInt(0) | input.getInt(0)
    }

    override def merge(buffer1:MutableAggregationBuffer,buffer2:Row):Unit={
        buffer1(0) = buffer1.getInt(0) | buffer2.getInt(0)
    }

    override def evaluate(buffer:Row):Any = buffer.getInt(0)
}



# 定义 bit_count 函数，该函数在spark 3.0.0 自带
def bit_count(i: Integer) = {
    var n = i
    n = n - ( ( n >> 1 ) & 1431655765 )
    n = ( n &  858993459 ) + ( ( n >> 2 ) &  858993459 )
    n = ( n &  252645135 ) + ( ( n >> 4 ) &  252645135 )
    n = n + ( n >> 8 )
    n = n + ( n >> 16 )
    n
}

# 注册自定义聚合函数
spark.udf.register("bit_or", new IntegerBitOr)

# 注册自定义函数
spark.udf.register("bit_count", bit_count _)

# 使用自定义函数
# spark.sql("select bit_or(num) from (select 1 as num union select 2 as num) as t").show
# spark.sql("select col_name, bit_count(col_name) as col_name2 from tble ")


spark.sql("""
select tb.consumption, tb.brandid, tb.marriage, tb.children, genderid, ageid, provinceid, tb.osdev, 
       255 - grouping_id(tb.consumption, tb.brandid, tb.marriage, tb.children, genderid, ageid, provinceid, tb.osdev) as prof_type, 
       COALESCE(tb.consumption, tb.brandid, tb.marriage, tb.children, genderid, ageid, provinceid, tb.osdev) as prof_val,
       sum(ta.panel_iutset) as uv, 
       sum(ta.panel_iutset * bit_count(ta.daybits)) as visitdays,
       sum(allcnt) as allcnt,
       sum(allsecs)/60 as allmins
from (select panel_id, max(panel_iutset) as panel_iutset, bit_or(daybits) as daybits,
             sum(panel_iutset * bit_count(daybits) * startups) as allcnt,
             sum(panel_iutset * bit_count(daybits) * effectivetime) as allsecs
        from gpbackup.gp91__irview_xut__dev35_month__bt_op
        where (backup_ts='20220407_234718' and backup_date_id=202202) and (status>=0 and date_id=202202)
        group by panel_id
) as ta
join gpbackup.gp91__irview_xut__dev35_month__bt_pd as tb on (tb.backup_ts='20220317_003341' and tb.backup_date_id=202202) and tb.date_id=202202 and tb.panel_id=ta.panel_id
group by tb.consumption, tb.brandid, tb.marriage, tb.children, genderid, ageid, provinceid, tb.osdev
grouping sets((tb.consumption), (tb.brandid), (tb.marriage), (tb.children), (genderid), (ageid), (provinceid), (tb.osdev))
""").show
