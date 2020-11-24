package jira;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *
 * 类型不匹配会报错：
 * Exception in thread "main" org.apache.calcite.runtime.CalciteContextException: From line 8, column 28 to line 8, column 55: Cannot apply '-' to arguments of type '<VARCHAR(2000)> - <INTERVAL SECOND>'. Supported form(s): '<NUMERIC> - <NUMERIC>'
 * '<DATETIME_INTERVAL> - <DATETIME_INTERVAL>'
 * '<DATETIME> - <DATETIME_INTERVAL>'
 * @author: zhushang
 * @create: 2020-11-09 10:38
 **/

public class WatermarkGenTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env, settings);
        tbEnv.sqlUpdate("create table t_kafka_consumer_input (\n" +
                "    `log_timestamp` BIGINT,\n" +
                "    `ip` STRING,\n" +
                "    `field` MAP<STRING, STRING>,\n" +
                "    --time1 as TO_TIMESTAMP(FROM_UNIXTIME(log_timestamp / 1000)),\n" +
                "    time2 as FROM_UNIXTIME(cast(`field`['timestamp'] as BIGINT)),\n" +
                "    --proctime as PROCTIME(),\n" +
                "    WATERMARK FOR time2 AS time2 - INTERVAL '10' SECOND\n" +
                ")  WITH (\n" +
                "   'properties.bootstrap.servers'='xxxx',\n" +
                "   'properties.group.id'='flink-sql-11-1',\n" +
                "   'scan.startup.mode'='group-offsets',\n" +
                "   'topic'='xxx',\n" +
                "   'connector'='kafka',\n" +
                "   'format'='json'" +
                ")");
    }
}
