/*
in_tablename: 表名
in_filter: 分区条件
in_backup_ts: hive 时间戳
*/
CREATE OR REPLACE FUNCTION "public"."hivebakup"("in_tablename" text, "in_filter" text, "in_backup_ts" text, "in_debug" int4)
  RETURNS "pg_catalog"."varchar" AS $BODY$
declare v_sql text;
declare v_pg_define text;
declare v_hive_define text;
declare v_column_list text;
declare v_server_name text = 'gp20';
declare v_gp_extname text = 'hivebak_'||replace(in_tablename, '.', '__')||'__ext';
declare v_hive_table text = v_server_name||'__'||current_database()||'__'||replace(in_tablename, '.', '__');
declare v_hdfs_location text = 'gphdfs://nameservice1/user/irsuser/gpbackup.db/'||v_hive_table||'/backup_ts='||in_backup_ts||'/backup_'||in_filter||'?compress=true';
declare v_hive_partname text = 'backup_'||(string_to_array(in_filter, '='))[1];
BEGIN

/*
调用方式：
select public.hivebakup('om_douyin.bt_douyin_mac_video_behavior_2019', 'date_id=20190101', '20211024_1643', 0);
*/


SELECT string_agg(attname || ' ' || pg_type_name, ', '   order by attnum) as pg_define, 
       string_agg(attname || ' ' || hive_type_name, ', ' order by attnum) as hive_define, 
       string_agg(attname, ', ' order by attnum) as column_list
INTO v_pg_define, v_hive_define, v_column_list
FROM (
    SELECT attname, attnum, format_type(atttypid, atttypmod) as pg_type_name,
           case (string_to_array(format_type(atttypid, atttypmod), '('))[1] 
           when 'smallint'          then 'smallint'
           when 'integer'           then 'int' 
           when 'bigint'            then 'bigint' 
           when 'double precision'  then 'double'
           when 'real'              then 'float'
           when 'timestamp'         then 'TIMESTAMP'
           when 'numeric'           then format_type(atttypid, atttypmod)
           else 'string'
           end as hive_type_name
    FROM pg_attribute
    WHERE attrelid=in_tablename::regclass
    AND attnum>0
) as ta;

v_sql = '
drop table if exists gpbackup.'||v_hive_table||';
create table gpbackup.'||v_hive_table||' (
    '||v_hive_define||'
)
PARTITIONED BY (backup_ts string, '||v_hive_partname||' int)
STORED as TEXTFILE;';
raise notice 'hql: %', replace(v_sql, chr(13), '');


v_sql = '
DROP EXTERNAL TABLE IF EXISTS product_ext.'||v_gp_extname||';
CREATE WRITABLE EXTERNAL TABLE product_ext.'||v_gp_extname||'
('||v_pg_define||')
LOCATION ('''||v_hdfs_location||''')
FORMAT ''TEXT'' (delimiter E''\x01'');';
if in_debug = 0 then
    execute v_sql;
else
    raise notice 'sql: %', replace(v_sql, chr(13), '');
end if;

v_sql = '
INSERT INTO product_ext.'||v_gp_extname||'
SELECT * FROM '||in_tablename||' WHERE '||in_filter||';';
if in_debug = 0 then
    execute v_sql;
else
    raise notice 'sql: %', replace(v_sql, chr(13), '');
end if;

RETURN 'done';

END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
