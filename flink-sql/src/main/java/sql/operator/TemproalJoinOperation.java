package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sql.dimension.MySqlDimension;
import sql.sink.ESSink;
import sql.source.KafkaSource;

/**
 * temproal table join,可以关联到历史维度值
 * 语法：FOR SYSTEM_TIME AS OF U.proctime
 * 截至1.10目前只支持proctime
 */
public class TemproalJoinOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        KafkaSource.getKafkaSource(tableEnv);//create kafka source table user_behavior
        MySqlDimension.getMysqlDim(tableEnv);//create mysql dimension table category_dim
        ESSink.getEsSink3(tableEnv);//create es sink table top_category
        Table table1 = tableEnv.sqlQuery("SELECT U.user_id, U.item_id, U.behavior, \n" +
                "  CASE C.parent_category_id\n" +
                "    WHEN 1 THEN '服饰鞋包'\n" +
                "    WHEN 2 THEN '家装家饰'\n" +
                "    WHEN 3 THEN '家电'\n" +
                "    WHEN 4 THEN '美妆'\n" +
                "    WHEN 5 THEN '母婴'\n" +
                "    WHEN 6 THEN '3C数码'\n" +
                "    WHEN 7 THEN '运动户外'\n" +
                "    WHEN 8 THEN '食品'\n" +
                "    ELSE '其他'\n" +
                "  END AS category_name\n" +
                "FROM user_behavior AS U LEFT JOIN category_dim FOR SYSTEM_TIME AS OF U.proctime AS C\n" +
                "ON U.category_id = C.sub_category_id");
        tableEnv.createTemporaryView("rich_user_behavior",table1);
        Table query = tableEnv.sqlQuery(
                "SELECT category_name, COUNT(*) buy_cnt\n" +
                        "FROM rich_user_behavior\n" +
                        "WHERE behavior = 'buy'\n" +
                        "GROUP BY category_name");
        query.executeInsert("top_category");
//        env.execute();
    }
}
