package sql.dimension;


import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MySqlDimension {
    public static void getMysqlDim(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.sqlUpdate("CREATE TABLE category_dim (\n" +
                "    sub_category_id BIGINT,  -- 子类目\n" +
                "    parent_category_id BIGINT -- 顶级类目\n" +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc',\n" +
                "    'connector.url' = 'jdbc:mysql://localhost:3307/flink',\n" +
                "    'connector.table' = 'category',\n" +
                "    'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
                "    'connector.username' = 'root',\n" +
                "    'connector.password' = '123456',\n" +
                "    'connector.lookup.cache.max-rows' = '5000',\n" +
                "    'connector.lookup.cache.ttl' = '10min'\n" +
                ")");
    }
}
