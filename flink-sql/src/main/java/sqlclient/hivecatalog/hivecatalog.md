### sqlclient集成hivecatalog的步骤
#### sql-client-defaults.yaml配置
```xml
catalogs: #[] # empty list
# A typical catalog definition looks like:
  - name: myhive
    type: hive
    hive-conf-dir: /Users/zhushang/Desktop/software/apache-hive-2.2.0-bin/conf
    hive-version: 2.2.0
```
#### 依赖包
flink-shaded-hadoop-2-uber-2.7.5-9.0.jar
flink-connector-hive_2.11-1.10.1.jar
hive-exec-2.2.0.jar
antlr-2.7.7.jar
antlr-runtime-3.4.jar
datanucleus-api-jdo-4.2.1.jar
datanucleus-core-4.1.6.jar
datanucleus-rdbms-4.1.7.jar
javax.jdo-3.2.0-m3.jar
除了前两个，其余都可以从hive的安装目录lib中获取到
