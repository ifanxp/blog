# PostgreSQL 学习笔记 - generate_series 函数
[官方文档](https://www.postgresql.org/docs/9.1/static/functions-srf.html)

### 作用：
返回从 start 到 top 范围的一个结果集，另外可以指定步长 (step)

### 有三种形式：
* ```generate_series(start, stop)``` 针对 int/bigint
* ```generate_series(start, stop, step)``` 针对 int/bigint
* ```generate_series(start, stop, step interval)``` 针对 timestamp

### 实例
```sql
SELECT * FROM generate_series(1, 5);
-----------------
1
2
3
4
5
```

```sql
SELECT * FROM generate_series(1, 5, 2);
-----------------
1
3
5
```

```sql
SELECT daytime::date as daytime FROM generate_series('2018-01-01'::date, '2018-01-07', '1 day') as d(daytime);
-----------------
2018-01-01
2018-01-02
2018-01-03
2018-01-04
2018-01-05
2018-01-06
2018-01-07
```