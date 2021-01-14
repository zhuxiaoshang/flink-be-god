package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author: zhushang
 * @create: 2021-01-06 10:29
 **/

public class DataGenTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.executeSql("CREATE TABLE orders (\n" +
                "order_uid BIGINT,\n" +
                "product_id BIGINT,\n" +
                "price DECIMAL(32, 2),\n" +
                "order_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "'connector' = 'datagen'\n" +
                ")");
        Table table = tableEnv.sqlQuery("select * from orders");
        tableEnv.toAppendStream(table, Row.class).print();
        env.execute();
    }
}
