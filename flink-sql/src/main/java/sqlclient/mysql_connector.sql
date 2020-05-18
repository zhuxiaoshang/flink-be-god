/**
连接mysql需要添加依赖到flink的lib目录
    mysql-connector-java-5.1.48.jar
    flink-jdbc_2.11-1.10.0.jar
 */
CREATE TABLE category_dim (
    sub_category_id BIGINT,  -- 子类目
    parent_category_id BIGINT -- 顶级类目
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://localhost:3306/flink',
    'connector.table' = 'category',
    'connector.driver' = 'com.mysql.jdbc.Driver',
    'connector.username' = 'root',
    'connector.password' = '123456',
    'connector.lookup.cache.max-rows' = '5000',
    'connector.lookup.cache.ttl' = '10min'
);