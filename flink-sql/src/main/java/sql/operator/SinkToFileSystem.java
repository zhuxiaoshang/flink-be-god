package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sql.sink.FileSystemSink;
import sql.source.KafkaSource;

/**
 * @author: zhushang
 * @create: 2020-10-21 19:40
 **/

public class SinkToFileSystem {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env, settings);
        KafkaSource.getKafkaSource(tbEnv);
        FileSystemSink.getFsSink(tbEnv);
        tbEnv.executeSql("insert into fs_sink select user_id,item_id,category_id,behavior from user_behavior");
    }
}
