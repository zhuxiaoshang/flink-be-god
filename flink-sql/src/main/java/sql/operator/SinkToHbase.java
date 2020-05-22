package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import sql.sink.ESSink;
import sql.sink.HbaseSink;
import sql.source.KafkaSource;

public class SinkToHbase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        KafkaSource.getKafkaSource(tableEnv);//create kafka source table user_behavior
        HbaseSink.getHbaseSink(tableEnv);
        Table table1 = tableEnv.sqlQuery("SELECT concat(user_id,item_id) as row_key,ROW(user_id, item_id,category_id," +
                "behavior) as col_family" +
                "\n" +
                "FROM user_behavior\n" +
                "WHERE behavior = 'buy'");
        table1.insertInto("hbaseSinkTable");
        env.execute();
    }
}
