package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import sql.source.KafkaSource;

/**
 * 普通groupby聚合，retract流
 */
public class GroupByOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        KafkaSource.getSourceTable(tableEnv);
        Table table1 = tableEnv.sqlQuery("SELECT behavior, COUNT(*)\n" +
                "FROM user_behavior\n" +
                "GROUP BY behavior");
        //group by是个retract流，聚合结果不断更新
        tableEnv.toRetractStream(table1, Row.class).print();
        env.execute();
    }
}
