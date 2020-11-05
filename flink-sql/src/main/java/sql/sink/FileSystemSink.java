package sql.sink;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: zhushang
 * @create: 2020-10-21 19:39
 **/

public class FileSystemSink {
    public static void getFsSink(StreamTableEnvironment tableEnvironment){
        tableEnvironment.executeSql("create table fs_sink(\n" +
                "\tuser_id BIGINT,\n" +
                "\titem_id BIGINT,\n" +
                "\tcategory_id BIGINT,\n" +
                "\tbehavior STRING\n" +
                ")with(\n" +
                "\t'connector' = 'filesystem', \n" +
                "\t'path' = 'hdfs://localhost:9000/test',\n" +
                "\t'format'='parquet',\n" +
                "\t'parquet.compression'='SNAPPY'\n" +
                ")");
    }
}
