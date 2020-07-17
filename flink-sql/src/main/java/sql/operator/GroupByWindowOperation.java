package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import sql.source.KafkaSource;

/**
 * 窗口聚合，append流
 */
public class GroupByWindowOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        KafkaSource.getKafkaSource(tableEnv);
        Table table1 = tableEnv.sqlQuery("SELECT item_id,TUMBLE_START(proctime, INTERVAL '10' SECOND), COUNT(*)\n" +
                "FROM user_behavior\n" +
                "WHERE behavior = 'buy'\n" +
                "GROUP BY TUMBLE(proctime, INTERVAL '10' SECOND),item_id");
        tableEnv.toAppendStream(table1, Row.class).print();
        env.execute();
    }
}
