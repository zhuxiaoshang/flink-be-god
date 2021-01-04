package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sql.sink.PrintSink;
import sql.source.KafkaSource;

/**
 * @author: zhushang
 * @create: 2020-12-30 15:12
 **/

public class AvroConfulentTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env, settings);
        KafkaSource.getKafkaAvroConfulent(tbEnv);
        PrintSink.getPrintSink(tbEnv);
        tbEnv.executeSql("insert into print_sink select * from user_behavior");
    }
}
