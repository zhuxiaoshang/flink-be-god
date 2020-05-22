package sql.sink;

import org.apache.flink.table.api.java.StreamTableEnvironment;

public class HbaseSink {
    public static void getHbaseSink(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.sqlUpdate("CREATE TABLE hbaseSinkTable (\n" +
                "  row_key String,\n" +
                "  col_family ROW<user_id BIGINT,item_id BIGINT,category_id BIGINT,behavior STRING>\n" +
                ") WITH (\n" +
                "  'connector.type' = 'hbase', -- required: specify this table type is hbase\n" +
                "  \n" +
                "  'connector.version' = '1.2.6',          -- required: valid connector versions are \"1.4.3\"\n" +
                "  'connector.table-name' = 'dfp_mem_lable_score_inf',  -- required: hbase table name\n" +
                "  \n" +
                "  'connector.zookeeper.quorum' = 'namenode1-sit.cnsuning.com,namenode2-sit.cnsuning.com,slave01-sit.cnsuning.com', -- required: HBase Zookeeper quorum configuration\n" +
                "  'connector.zookeeper.znode.parent' = '/hbase',    -- optional: the root dir in Zookeeper for HBase cluster.\n" +
                "                                                   -- The default value is \"/hbase\".\n" +
                "\n" +
                "  'connector.write.buffer-flush.max-size' = '10mb', -- optional: writing option, determines how many size in memory of buffered\n" +
                "                                                    -- rows to insert per round trip. This can help performance on writing to JDBC\n" +
                "                                                    -- database. The default value is \"2mb\".\n" +
                "\n" +
                "  'connector.write.buffer-flush.max-rows' = '1000', -- optional: writing option, determines how many rows to insert per round trip.\n" +
                "                                                    -- This can help performance on writing to JDBC database. No default value,\n" +
                "                                                    -- i.e. the default flushing is not depends on the number of buffered rows.\n" +
                "\n" +
                "  'connector.write.buffer-flush.interval' = '2s',   -- optional: writing option, sets a flush interval flushing buffered requesting\n" +
                "                                                    -- if the interval passes, in milliseconds. Default value is \"0s\", which means\n" +
                "                                                    -- no asynchronous flush thread will be scheduled.\n" +
                ")");
    }
}
