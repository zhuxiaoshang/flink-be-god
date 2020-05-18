/**--kafka 数据源
连接kafka需要依赖
flink-sql-connector-kafka_2.11-1.10.0.jar
json格式数据需要依赖
flink-json-1.10.0.jar
 */
CREATE TABLE user_behavior (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    ts TIMESTAMP(3),
    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列
    WATERMARK FOR ts as ts - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列
) WITH (
    'connector.type' = 'kafka',  -- 使用 kafka connector
    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本
    'connector.topic' = 'user_behavior',  -- kafka topic
    'connector.startup-mode' = 'earliest-offset',  -- 从起始 offset 开始读取
    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zookeeper 地址
    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址
    'format.type' = 'json'  -- 数据源格式为 json
);
