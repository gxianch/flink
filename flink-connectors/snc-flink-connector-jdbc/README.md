### 开发

改造自flink 1.15 jdbc (flink-connetor-jdbc) 源码(jdbc支持oracle)，以适配目前的flink1.13.5版本（jdbc不支持oracle)

1.引入flink 1.15上的类和方法存放在 org.apache.flink.connector.jdbc.snc包下
2.改造的地方原代码保留已注释，用于后续问题排查

### 实施

```
oracle包ojdbc8-12.2.0.1.jar(mysql-connector-java-8.0.27.jar),和本项目打包的snc-flink-connector-jdbc-1.13-SNAPSHOT.jar同时复制到lib目录下
```

### 测试

```sql
### oracle
CREATE TABLE MyUserTableoracle (
  ID STRING,
  NAME STRING,
  AGEXX STRING
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:oracle:thin:@xx.6:1521:ORCL',
   'table-name' = 'DATAWARE."users"',
   'username' = 'dataware',
   'password' = 'dataware'
);
SELECT ID, NAME, AGEXX FROM MyUserTableoracle;
//oracle要求字段大写

### mysql
-- 在 Flink SQL 中注册一张 MySQL 表 'users'
CREATE TABLE MyUserTable (
  id BIGINT,
  name STRING,
  age INT,
  status BOOLEAN,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://xx.101:3306/guo_test',
   'table-name' = 'users',
   'username' = 'username',
   'password' = 'password'
);
SELECT id, name, age, status FROM MyUserTable;


```

