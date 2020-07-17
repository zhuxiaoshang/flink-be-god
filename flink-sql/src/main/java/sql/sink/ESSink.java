package sql.sink;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ESSink {
    public static void getEsSink1(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql("CREATE TABLE buy_cnt_per_hour ( \n" +
                "    hour_of_day BIGINT,\n" +
                "    buy_cnt BIGINT\n" +
                ") WITH (\n" +
                "    'connector.type' = 'elasticsearch', -- 使用 elasticsearch connector\n" +
                "    'connector.version' = '6',  -- elasticsearch 版本，6 能支持 es 6+ 以及 7+ 的版本\n" +
                "    'connector.hosts' = 'http://localhost:9200',  -- elasticsearch 地址\n" +
                "    'connector.index' = 'buy_cnt_per_hour',  -- elasticsearch 索引名，相当于数据库的表名\n" +
                "    'connector.document-type' = 'user_behavior', -- elasticsearch 的 type，相当于数据库的库名\n" +
                "    'connector.bulk-flush.max-actions' = '1',  -- 每条数据都刷新\n" +
                "    'format.type' = 'json',  -- 输出数据格式 json\n" +
                "    'update-mode' = 'append'\n" +
                ")");
    }
    public static void getEsSink2(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql("CREATE TABLE cumulative_uv (\n" +
                "    time_str STRING,\n" +
                "    uv BIGINT\n" +
                ") WITH (\n" +
                "    'connector.type' = 'elasticsearch',\n" +
                "    'connector.version' = '6',\n" +
                "    'connector.hosts' = 'http://localhost:9200',\n" +
                "    'connector.index' = 'cumulative_uv',\n" +
                "    'connector.document-type' = 'user_behavior',\n" +
                "    'format.type' = 'json',\n" +
                "    'update-mode' = 'upsert'\n" +
                ")");
    }
    public static void getEsSink3(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql("CREATE TABLE top_category (\n" +
                "    category_name STRING,  -- 类目名称\n" +
                "    buy_cnt BIGINT  -- 销量\n" +
                ") WITH (\n" +
                "    'connector.type' = 'elasticsearch',\n" +
                "    'connector.version' = '6',\n" +
                "    'connector.hosts' = 'http://localhost:9200',\n" +
                "    'connector.index' = 'top_category',\n" +
                "    'connector.document-type' = 'user_behavior',\n" +
                "    'format.type' = 'json',\n" +
                "    'update-mode' = 'upsert'\n" +
                ") ");
    }
}
