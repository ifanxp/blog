记一次Greenplum数据迁移


```bash
# 生成数据库脚本
pg_dump -h <host> -p <port> -U <user_name> -d <database> -n <schema> -s -f gpdb_definition.sql

# 在目标服务器执行脚本
psql -h <host> -p <port> -d <database> -U <user_name> -f gpdb_definition.sql
psql <database> -f gpdb_definition.sql #在本机上可以这样
```

```sql
-- 在数据库中创建一个视图，列出所有需要同步的表名
-- 分区表：只列出各分区，避免重复插入
create view <schema>.<view_name> as
select '"' || ns.nspname || '"' || '."' || tb.relname || '"' as table_name, coalesce(pt.table_type, 'normal_table') as table_type
from pg_class as tb
join pg_namespace as ns on ns.oid = tb.relnamespace
left join (
     select schemaname, partitiontablename as tablename, 'partition' as table_type
     from pg_partitions 
) as pt on pt.schemaname = ns.nspname and pt.tablename = tb.relname
where tb.relkind='r' and ns.nspname=<schema>
```



```bash
#!/bin/bash
#save to import_data.sh
if [ $# = 0 ]; then
   echo "Usage: sh import_data.sh <filename>"
   exit 1
fi

for s in $(cat "$1")
do
    if [[ $s = *"product."* ]]; then
       psql -h <host> -p <port> -d <database) -U <user_name> -c "copy $s to stdout  csv" | psql -h <host> -p <port> -d <database> -c "copy $s from stdin csv" 
       
       # 这样可以压缩并保存至磁盘（gzip格式）
       # psql -h <host> -p <port> -d <database> -U <user_name> -c "copy <schema>.<view_name> to stdout csv"|gzip > <schema>.<view_name>.gz
    fi
done
```



```bash
# 从上面定义的视图里面列出所有待同步的表名
psql -h <host> -p <port> -d <database> -U <user_name> -c "select * From <schema>.<view_name> where table_type='normal_table'" > tables.txt
sh import_data.sh
```
