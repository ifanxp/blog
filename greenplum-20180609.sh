#!/bin/bash
# 需要用到的管理视图，来自 https://blog.csdn.net/shipeng1022/article/details/78720867
# DROP VIEW IF EXISTS public.dba_show_tables_need_vacuum;
# CREATE VIEW public.dba_show_tables_need_vacuum AS
#     SELECT nspname as schema_name, relname AS table_name, avg(percent_hidden) avg_percent_hidden, max(percent_hidden) max_percent_hidden
#       FROM (SELECT t2.nspname, t1.relname, (gp_toolkit.__gp_aovisimap_compaction_info(t1.oid)).*   
#               FROM pg_class t1, pg_namespace t2 
#              WHERE t1.relnamespace=t2.oid
#                AND relstorage in ('c', 'a')   
#            ) t   
#      WHERE t.percent_hidden > 0.2
#      GROUP BY nspname, relname
#      ORDER BY avg_percent_hidden desc;

if [[ $# = 0 ]]; then
   echo "Usage: sh auto_vacuum.sh <database> [schema]"
   exit 1
fi

todo=todo.list
err=err.log
log=done.log
db=$1
schema=""
dbv_schema=public
dbv_view=dba_show_tables_need_vacuum

if [[ $# = 2 ]]; then
   schema=$2
fi

# 指定的数据库是否存在
db_exists=`psql postgres -t -c "SELECT 1 FROM pg_database WHERE datname='$db'"|grep 1|wc -l`
if [[ $db_exists = 0 ]]; then
   echo "database [$db] not exists"
   exit 1
fi

# 管理视图public.dba_show_tables_need_vacuum是否存在
view_exists=`psql $db -t -c "SELECT 1 FROM pg_views WHERE schemaname='$dbv_schema' AND viewname='$dbv_view'"|grep 1|wc -l`
if [[ $view_exists = 0 ]]; then
   echo "view [$dbv_schema.$dbv_view] not exists"
   exit 1
fi

if [[ $schema = "" ]]; then
    psql $db -t -c "SELECT schema_name || '.' || table_name AS table_name FROM $dbv_schema.$dbv_view" 1>$todo 2>$err
else
    psql $db -t -c "SELECT schema_name || '.' || table_name AS table_name FROM $dbv_schema.$dbv_view WHERE schema_name='$schema'" 1>$todo 2>$err
fi

echo "start auto vacuum in $db:`date '+%Y-%m-%d %H:%M:%S'`" > $log
for table in $(grep "^[[:space:]]\{1,\}\w\{1,\}\.\w\{1,\}$" $todo)
do
    if [[ $table = *"."* ]]; then
        #psql $db -c "VACUUM ANALYSE $table"
        #echo "VACUUM ANALYSE $table" >> $log
        echo "VACUUM ANALYSE $table"
    fi
done
echo "done auto vacuum in $db:`date '+%Y-%m-%d %H:%M:%S'`" >> $log
