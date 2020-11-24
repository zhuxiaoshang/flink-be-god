package sql.hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Connect2Hive {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);
        env.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);

        String name = "myhive";
        String defaultDatabase = "my_db";
        // a local path
        String hiveConfDir = "/Users/zhushang/Desktop/software/apache-hive-2.2.0-bin/conf";
        String version = "2.2.0 ";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hive);
        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");
        tableEnv.executeSql("DROP TABLE IF EXISTS kafka_source");
        tableEnv.executeSql("CREATE TABLE kafka_source (\n" +
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
                "    'format.type' = 'json'"+
//                "    'connector' = 'kafka',  -- 使用 kafka connector\n" +
//                "    'topic' = 'user_behavior',  -- kafka topic\n" +
//                "    'scan.startup.mode' = 'earliest-offset',  -- 从起始 offset 开始读取\n" +
//                "    'properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址\n" +
//                "    'format' = 'json'  -- 数据源格式为 json\n" +
                ")");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS flink_hive");
        tableEnv.executeSql("DROP TABLE IF EXISTS my_db.flink_hive");
        tableEnv.executeSql("CREATE EXTERNAL TABLE my_db.flink_hive (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING\n" +
                ") PARTITIONED BY (`day` STRING, `hour` STRING) STORED AS PARQUET\n" +
                "TBLPROPERTIES (\n" +
                "    'parquet.compression'='SNAPPY',\n" +
                "    'sink.partition-commit.policy.kind' = 'metastore,success-file,custom',\n" +
                "    'sink.partition-commit.success-file.name' = '_SUCCESS',\n" +
                "    'sink.partition-commit.policy.class' = 'sql.hive.commit.policy.ParquetFileMergingCommitPolicy'\n" +
                ")");

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("insert into my_db.flink_hive\n" +
                "select\n" +
                "    user_id,\n" +
                "    item_id,\n" +
                "    category_id,\n" +
                "    behavior,\n" +
                "    DATE_FORMAT(ts,'yyyy-MM-dd') as `day`,\n" +
                "    DATE_FORMAT(ts,'HH') as `hour`\n" +
                "from kafka_source");

    }
}
