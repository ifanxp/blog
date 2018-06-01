```sql
CREATE OR REPLACE FUNCTION "public"."udf_save_csv"("p_tablename" text, "p_debug" int4)
  RETURNS "pg_catalog"."int4" AS $BODY$
DECLARE
	v_schemaname text;
	v_tablename text;
    v_rowcount integer;
    v_sql_create text;
    v_sql_insert text;
    v_columns text;
BEGIN
--------------------------------------------------------------------------------

SELECT s.nspname as schemaname, t.relname as tablename
INTO v_schemaname, v_tablename
FROM pg_class as t
JOIN pg_namespace as s on s.oid = t.relnamespace
WHERE t.oid=p_tablename::regclass;

raise notice 'table: %.%', v_schemaname, v_tablename;

SELECT string_agg(attname || ' ' || format_type(atttypid, atttypmod), ', ' order by attnum), 
       string_agg(attname, ', ' order by attnum)
INTO v_sql_create, v_columns
FROM pg_attribute
WHERE attrelid=(v_schemaname || '.' || v_tablename)::regclass
AND attnum>0;

v_sql_create := '
DROP EXTERNAL TABLE IF EXISTS product_ext.' || v_tablename || ';
CREATE WRITABLE EXTERNAL TABLE product_ext.' || v_tablename || ' (' || v_sql_create || ') 
LOCATION (''gpfdist://<host>:<port>/<dir>/' || v_schemaname || '.' || v_tablename || '.csv'')
FORMAT ''TEXT'' (DELIMITER E''' || E'\\' || 'x01'');
';

v_sql_insert := '
INSERT INTO product_ext.' || v_tablename || ' (' || v_columns || ')
SELECT ' || v_columns || '
FROM ' || v_schemaname || '.' || v_tablename || ';
';

IF p_debug = 0 THEN
    EXECUTE v_sql_create;
	EXECUTE v_sql_insert;
	GET DIAGNOSTICS v_rowcount = ROW_COUNT;
END IF;

raise notice '请检查SQL % %', v_sql_create, v_sql_insert;

RETURN v_rowcount;
--------------------------------------------------------------------------------
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
```

```sql
CREATE OR REPLACE FUNCTION "public"."udf_load_csv"("p_tablename" text, "p_debug" int4)
  RETURNS "pg_catalog"."int4" AS $BODY$
DECLARE
	v_schemaname text;
	v_tablename text;
    v_rowcount text;
    v_sql_create text;
    v_sql_insert text;
    v_columns text;
BEGIN
--------------------------------------------------------------------------------

SELECT s.nspname as schemaname, t.relname as tablename
INTO v_schemaname, v_tablename
FROM pg_class as t
JOIN pg_namespace as s on s.oid = t.relnamespace
WHERE t.oid=p_tablename::regclass;

raise notice 'table: %.%', v_schemaname, v_tablename;

SELECT string_agg(attname || ' ' || format_type(atttypid, atttypmod), ', ' order by attnum), 
       string_agg(attname, ', ' order by attnum)
INTO v_sql_create, v_columns
FROM pg_attribute
WHERE attrelid=(v_schemaname || '.' || v_tablename)::regclass
AND attnum>0;

v_sql_create := '
DROP EXTERNAL TABLE IF EXISTS product_ext.' || v_tablename || ';
CREATE EXTERNAL TABLE product_ext.' || v_tablename || ' (' || v_sql_create || ') 
LOCATION (''gpfdist://<host>:<port>/<dir>/' || v_schemaname || '.' || v_tablename || '.csv'')
FORMAT ''TEXT'' (DELIMITER E''' || E'\\' || 'x01'');
';

v_sql_insert := '
INSERT INTO ' || v_schemaname || '.' || v_tablename || ' (' || v_columns || ')
SELECT ' || v_columns || '
FROM product_ext.' || v_tablename || ';
';

IF p_debug = 0 THEN
    EXECUTE v_sql_create;
	EXECUTE v_sql_insert;
	GET DIAGNOSTICS v_rowcount = ROW_COUNT;
END IF;

raise notice '请检查SQL % %', v_sql_create, v_sql_insert;

RETURN v_rowcount;
--------------------------------------------------------------------------------
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
```

```shell
#!/bin/bash
#movedata.sh

if [ $# = 0 ]; then
   echo "Usage: sh movedata.sh <table list filename>"
   exit 1
fi

tablelist=$1

# thread
maxnum=2
duration=5
currnum=0

for table in $(cat "$tablelist")
do
    echo "table: $table"
    ps aux|grep psql|awk '{print substr($0, index($0, "public.udf"))}' > running.log
    cat running.log
    currnum=`cat running.log|wc -l`
    while [[ $currnum -ge $maxnum ]]; do
            echo "maxnum=$maxnum current=$currnum wait for $duration seconds..."
            sleep $duration
            ps aux|grep psql|awk '{print substr($0, index($0, "public.udf"))}' > running.log
            cat running.log
            currnum=`cat running.log|wc -l`
    done
    #nohup sh movetable.sh $table >> info.log 2>>err.log &
    sleep $duration
done

ps aux|grep psql|awk '{print substr($0, index($0, "public.udf"))}' > running.log
cat running.log
currnum=`cat running.log|wc -l`
while [[ $currnum -ge $maxnum ]]; do
		echo "maxnum=$maxnum current=$currnum wait for $duration seconds..."
		sleep $duration
		ps aux|grep psql|awk '{print substr($0, index($0, "public.udf"))}' > running.log
    cat running.log
		currnum=`cat running.log|wc -l`
done
```


```shell
#!/bin/bash

if [ $# = 0 ]; then
   echo "Usage: sh movetable.sh <table name>"
   exit 1
fi

table=$1

# data source
fserver=<source:server>
fport=<source:port>
fdb=<source:database>
fuser=<source:role>

# data destination
nserver=<destination:server>
nport=<destination:port>
ndb=<destination:database>
nuser=<destination:role>

echo "start $table:`date '+%Y-%m-%d %H:%M:%S'`"
psql -h $fserver -p $fport -U $fuser $fdb -v "ON_ERROR_STOP=1" -c "SELECT public.udf_save_csv('$table', 0);" >>info.log 2>>error.log
psql -h $nserver -p $nport -U $nuser $ndb -v "ON_ERROR_STOP=1" -c "SELECT public.udf_load_csv('$table', 0);" >>info.log 2>>error.log
echo "end $table:`date '+%Y-%m-%d %H:%M:%S'`"
```

