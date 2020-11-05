package application.filesystem;


import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 为验证streamfilesink什么情况下需要hadoop2.7+版本
 * https://ci.apache.org/projects/flink/flink-docs-master/dev/connectors/streamfile_sink.html#general
 *
 * 结论：在使用StreamingFileSink.forRowFormat + DefaultRollingPolicy 时才会对hadoop版本有要求。
 * 使用CheckpointRollingPolicy没有此问题。
 * 详细过程请见：https://blog.csdn.net/weixin_41608066/article/details/109230391
 * @author: zhushang
 * @create: 2020-10-22 14:14
 **/

public class Kafka2Hdfs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "xxx:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> src = env
                .addSource(new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties));


//        src.addSink(StreamingFileSink
//                .forRowFormat(
//                        new Path("hdfs://xxx/zs_test"),
//                        new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(DefaultRollingPolicy.builder().build()).build());

        src.addSink(StreamingFileSink
                .forRowFormat(
                        new Path("hdfs://xxx/zs_test"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build()).build());

        env.execute("sink to hdfs");
    }
}
