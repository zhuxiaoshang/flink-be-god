package sql.batch;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author: zhushang
 * @create: 2020-12-31 14:14
 **/

public class BatchExecution {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.executeSql("CREATE TABLE user_behavior (\n" +
                "  user_id BIGINT,\n" +
                "  item_id BIGINT,\n" +
                "  category_id BIGINT,\n" +
                "  behavior STRING,\n" +
                "  ts TIMESTAMP(3),\n" +
                "  proctime proctime()" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'user_behavior',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'json',\n" +
                " 'json.fail-on-missing-field' = 'false',\n" +
                " 'json.ignore-parse-errors' = 'true'\n" +
                ")");
        tEnv.executeSql("CREATE TABLE PRINT () WITH ('connector' = 'print') LIKE user_behavior (EXCLUDING ALL\n" +
                "    INCLUDING GENERATED)");
        tEnv.executeSql("insert into print select * from user_behavior");
    }
}
