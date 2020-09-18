package jira;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * https://issues.apache.org/jira/browse/FLINK-19281
 * UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(sqlTableLike.getSourceTable()
 * 			.toString());
 * 			改成
 * 		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(sqlTableLike.getSourceTable()
 *  * 			.names);
 * @author: zhushang
 * @create: 2020-09-18 11:45
 **/

public class FLINK_19281 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env, settings);
        tbEnv.executeSql("CREATE TABLE kafka_source (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3),\n" +
                "    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列\n" +
                "    WATERMARK FOR ts as ts - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',  -- 使用 kafka connector\n" +
                "    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
                "    'connector.topic' = 'user_behavior',  -- kafka topic\n" +
                "    'connector.startup-mode' = 'earliest-offset',  -- 从起始 offset 开始读取\n" +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zookeeper 地址\n" +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址\n" +
                "    'format.type' = 'json'  -- 数据源格式为 json\n" +
                ")");
        tbEnv.executeSql("CREATE TABLE kafka_source1 like default_catalog.default_database.kafka_source");
    }
}
