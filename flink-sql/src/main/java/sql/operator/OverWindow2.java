package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: zhushang
 * @create: 2020-09-14 17:22
 **/

public class OverWindow2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env, settings);
        tbEnv.executeSql("CREATE TABLE kafka_source (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING,\n" +
                "    ts BIGINT,\n" +
                "    proctime as PROCTIME()\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',  -- 使用 kafka connector\n" +
                "    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
                "    'connector.topic' = 'test3',  -- kafka topic\n" +
                "    'connector.startup-mode' = 'earliest-offset',  -- 从起始 offset 开始读取\n" +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zookeeper 地址\n" +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址\n" +
                "    'format.type' = 'json' -- 数据源格式为 json\n" +
                ")");
        tbEnv.executeSql("create table print_sink(cou BIGINT) with ('connector' = 'print')");
        tbEnv.executeSql("insert into print_sink select SUM(item_id) OVER (\n" +
                "  PARTITION BY user_id\n" +
                "  ORDER BY proctime\n" +
                "  ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)\n" +
                "FROM kafka_source");
    }
}
