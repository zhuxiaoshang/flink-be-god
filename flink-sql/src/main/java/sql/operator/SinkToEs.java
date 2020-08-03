package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sql.sink.ESSink;
import sql.source.KafkaSource;

public class SinkToEs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        KafkaSource.getKafkaSource(tableEnv);//create kafka source table user_behavior
        ESSink.getEsSink1(tableEnv);//create es sink table buy_cnt_per_hour
        Table table1 = tableEnv.sqlQuery("SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)\n" +
                "FROM user_behavior\n" +
                "WHERE behavior = 'buy'\n" +
                "GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)");
        table1.executeInsert("buy_cnt_per_hour");
//        env.execute();
    }
}
