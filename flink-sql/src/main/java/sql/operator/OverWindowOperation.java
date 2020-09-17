package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sql.sink.ESSink;
import sql.source.KafkaSource;

/**
 * over window可以对当前数据前有界（或无界）到当前数据之间的数据集进行操作
 *
 * demo链接
 * http://wuchong.me/blog/2020/02/25/demo-building-real-time-application-with-flink-sql/#%E7%BB%9F%E8%AE%A1%E4%B8%80%E5%A4%A9%E6%AF%8F10%E5%88%86%E9%92%9F%E7%B4%AF%E8%AE%A1%E7%8B%AC%E7%AB%8B%E7%94%A8%E6%88%B7%E6%95%B0
 */
public class OverWindowOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        KafkaSource.getKafkaSource(tableEnv);//create kafka source table user_behavior
        ESSink.getEsSink2(tableEnv);//create es sink table cumulative_uv
        //1.统计0点到当前行的累计uv数，以及当前行所在的十分钟，并存到临时视图中
        Table table1 = tableEnv.sqlQuery("SELECT \n" +
                "  MAX(SUBSTR(DATE_FORMAT(ts, 'HH:mm'),1,4) || '0') OVER w AS time_str, \n" +
                "  COUNT(DISTINCT user_id) OVER w AS uv\n" +
                "FROM user_behavior\n" +
                "WINDOW w AS (ORDER BY proctime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
        tableEnv.createTemporaryView("uv_per_10min",table1);
        //2.统计每10分钟的最大uv数
        tableEnv.executeSql("INSERT INTO cumulative_uv\n" +
                "SELECT time_str, MAX(uv)\n" +
                "FROM uv_per_10min\n" +
                "GROUP BY time_str");
        //env.execute();不需要再调用
        //tableEnv.execute("name");
    }
}
