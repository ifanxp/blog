查看正在执行的 SQL

```sql
SELECT
    c.session_id, c.net_transport, c.encrypt_option,   
    c.auth_scheme, c.client_net_address, s.host_name, s.program_name,   
    s.client_interface_name, s.login_name, s.nt_domain,   
    s.nt_user_name, s.original_login_name, c.connect_time,   
    s.login_time,q.text
FROM sys.dm_exec_connections AS c  
JOIN sys.dm_exec_sessions AS s  
    ON c.session_id = s.session_id  
cross apply fn_get_sql(most_recent_sql_handle) q
```