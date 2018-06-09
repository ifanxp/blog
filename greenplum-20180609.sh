#!/bin/bash

if [[ $# = 0 ]]; then
   echo "Usage: sh auto_vacuum.sh <database>"
   exit 1
fi

todo=todo.list
err=err.log
log=done.log
db=$1
dbv_schema=public
dbv_view=dba_show_tables_need_vacuum

db_exists=`psql -t -c "SELECT 1 FROM pg_database WHERE datname='$db'"|grep 1|wc -l`
if [[ $db_exists = 0 ]]; then
   echo "database [$db] not exists"
   exit 1
fi

view_exists=`psql $db -t -c "SELECT 1 FROM pg_views WHERE schemaname='$dbv_schema' AND viewname='$dbv_view'"|grep 1|wc -l`
if [[ $view_exists = 0 ]]; then
   echo "view [$dbv_schema.$dbv_view] not exists"
   exit 1
fi

psql $db -t -c "SELECT schema_name || '.' || table_name AS table_name FROM $dbv_schema.$dbv_view" 1>$todo 2>$err

echo "start auto vacuum in $db:`date '+%Y-%m-%d %H:%M:%S'`" > $log
for table in $(grep "^[[:space:]]\{1,\}\w\{1,\}\.\w\{1,\}$" $todo)
do
    if [[ $table = *"."* ]]; then
        psql $db -c "VACUUM ANALYSE $table"
        echo "VACUUM ANALYSE $table" >> $log
    fi
done
echo "done auto vacuum in $db:`date '+%Y-%m-%d %H:%M:%S'`" >> $log
