/**
连接es需要添加依赖
    flink-sql-connector-elasticsearch6_2.11-1.10.0.jar

 */
CREATE TABLE buy_cnt_per_hour (
    hour_of_day BIGINT,
    buy_cnt BIGINT
) WITH (
    'connector.type' = 'elasticsearch', -- 使用 elasticsearch connector
    'connector.version' = '6',  -- elasticsearch 版本，6 能支持 es 6+ 以及 7+ 的版本
    'connector.hosts' = 'http://localhost:9200',  -- elasticsearch 地址
    'connector.index' = 'buy_cnt_per_hour',  -- elasticsearch 索引名，相当于数据库的表名
    'connector.document-type' = 'user_behavior', -- elasticsearch 的 type，相当于数据库的库名
    'connector.bulk-flush.max-actions' = '1',  -- 每条数据都刷新
    'format.type' = 'json',  -- 输出数据格式 json
    'update-mode' = 'append'
);