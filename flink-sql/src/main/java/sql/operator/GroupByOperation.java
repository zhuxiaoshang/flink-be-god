package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import sql.source.KafkaSource;

/**
 * 普通groupby聚合，retract流
 */
public class GroupByOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tbEnv = TableEnvironment.create(settings);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        KafkaSource.getKafkaSource(tbEnv);
        Table table1 = tbEnv.sqlQuery("SELECT behavior, COUNT(*)\n" +
                "FROM user_behavior\n" +
                "GROUP BY behavior");
        //group by是个retract流，聚合结果不断更新
//        tableEnv.toRetractStream(table1, Row.class).print();
        tbEnv.sqlUpdate("insert into user_behavior select * from user_behavior");
        env.execute();
    }
}
