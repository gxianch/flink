### 开发

改造自flink 1.13 jdbc (flink-connetor-jdbc) 源码，增加orace支持
部分方法来源于flink 1.15版本，因为flink 1.15 已实现支持oracle



### 实施和测试

oracle包ojdbc8-12.2.0.1.jar(mysql-connector-java-8.0.27.jar),和本项目打包的snc-flink-connector-jdbc-oracle-1.13-SNAPSHOT.jar同时复制到lib目录下
测试 sql-shell 
./bin/start-cluster.sh
./bin/sql-client.sh
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

