package sql.sink;


import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class HbaseSink {
    public static void getHbaseSink(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql("CREATE TABLE hbaseSinkTable (\n" +
                "    row_key string,\n" +
                "    cf ROW(user_id BIGINT,item_id BIGINT,category_id BIGINT,behavior STRING)\n" +
                ") WITH (\n" +
                "    'connector.type' = 'hbase',\n" +
                "    -- 目前只支持 1.4.3 ，   HBaseValidator.CONNECTOR_VERSION_VALUE_143 写死了 1.4.3, 其他版本可以正常写数到 " +
                "hbase\n" +
                "    -- 生产慎用\n" +
                "    'connector.version' = '1.4.3',                    -- hbase vesion\n" +
                "    'connector.table-name' = 'hbase_test',                  -- hbase table name\n" +
                "    'connector.zookeeper.quorum' = 'localhost:2181',       -- zookeeper quorum\n" +
                "    'connector.zookeeper.znode.parent' = '/hbase',    -- hbase znode in zookeeper\n" +
                "    'connector.write.buffer-flush.max-size' = '10mb', -- max flush size\n" +
                "    'connector.write.buffer-flush.max-rows' = '1000', -- max flush rows\n" +
                "    'connector.write.buffer-flush.interval' = '2s'    -- max flush interval\n" +
                ")");
    }
}
