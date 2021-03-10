package application.iceberg;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author: zhushang
 * @create: 2021-03-10 00:03
 **/

public class Mysql2Iceberg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(getMysqlCdc()).print().setParallelism(1);
        env.execute();

    }

    private static SourceFunction getMysqlCdc() {
        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("test") // monitor all tables under inventory database
                .username("root")
                .password("123456")
                .tableList("test.person")//注意用db.table
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                .build();
        return sourceFunction;
    }
}
