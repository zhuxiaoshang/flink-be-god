package sql.sink;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: zhushang
 * @create: 2020-08-27 00:20
 **/

public class JDBCSink {
    public static void getJDBCSink(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql("CREATE TABLE mysqltable (\n" +
                "  user_id BIGINT,\n" +
                "  item_id BIGINT\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3307/flink',\n" +
                "   'driver' = 'com.mysql.jdbc.Driver',\n" +
                "   'table-name' = 'jdbctable',\n" +
                "   'username' = 'root',\n"+
                "   'password' = '123456'"+
                ")");
    }
}
