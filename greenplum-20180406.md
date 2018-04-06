# PostgreSQL 学习笔记 - generate_series 函数
[官方文档](https://www.postgresql.org/docs/9.1/static/functions-srf.html)

### 作用：
返回从 start 到 top 范围的一个结果集，另外可以指定步长 (step)

### 有三种形式：
* ```generate_series(start, stop)``` 针对 int/bigint
* ```generate_series(start, stop, step)``` 针对 int/bigint
* ```generate_series(start, stop, step interval)``` 针对 timestamp
