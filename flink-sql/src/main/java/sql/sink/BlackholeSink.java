package sql.sink;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class BlackholeSink {
    public static void getBlackholeSink(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql("create table blackhole_table(f_sequence BIGINT,f_random BIGINT,f_random_str STRING) with('connector' = 'blackhole')");
    }
}
