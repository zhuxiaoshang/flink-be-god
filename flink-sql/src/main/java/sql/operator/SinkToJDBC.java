package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sql.sink.JDBCSink;
import sql.source.KafkaSource;

/**
 * @author: zhushang
 * @create: 2020-08-27 00:21
 **/

public class SinkToJDBC {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env, settings);
        KafkaSource.getKafkaSource(tbEnv);
        JDBCSink.getJDBCSink(tbEnv);
        tbEnv.executeSql("insert into mysqltable select user_id,item_id from user_behavior");
    }
}
