package sql.sink;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: zhushang
 * @create: 2020-10-21 19:39
 **/

public class PrintSink {
    public static void getPrintSink(StreamTableEnvironment tableEnvironment){
        tableEnvironment.executeSql("create table print_sink(\n" +
                "\tuser_id BIGINT,\n" +
                "\titem_id BIGINT,\n" +
                "\tcategory_id BIGINT,\n" +
                "\tbehavior STRING\n" +
                ")with(\n" +
                "\t'connector' = 'print' \n" +
                ")");
    }
}
