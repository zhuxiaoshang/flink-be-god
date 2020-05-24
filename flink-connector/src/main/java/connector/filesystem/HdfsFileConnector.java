package connector.filesystem;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;



/**
 * Stream file方式 写入hdfs
 */
public class HdfsFileConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        DataStream<String> src = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (true) {
                    ctx.collect("sdfsf ttttt");
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });
        SingleOutputStreamOperator<String> map = src.flatMap(new FlatMapFunction<String,
                String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String ss : s.split(" ")) {
                    collector.collect(ss);
                }
            }
        });
        /**
         * 此方法已废弃
        BucketingSink<String> sink = new BucketingSink<String>("hdfs://namenode:9000/tmp/zs");
        sink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd--HHmm", ZoneId.of("Asia/Shanghai")));
        sink.setWriter(new StringWriter<>());
        sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,
        sink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins
        */

        /**
         * 注意 localhost:9000为namenode的地址，需要配置相应的滚动策略
         */
        map.addSink(StreamingFileSink.forRowFormat(new Path("hdfs://localhost:9000/test"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build()).build());

        env.execute();
/**
 * 执行结果如下
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-4-2
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-4-3
 * -rw-r--r--   3 zhushang supergroup         24 2020-05-25 00:50 /test/2020-05-25--00/part-4-4
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-4-5
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:51 /test/2020-05-25--00/part-4-6
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-5-0
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-5-1
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-5-2
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-5-3
 * -rw-r--r--   3 zhushang supergroup         24 2020-05-25 00:50 /test/2020-05-25--00/part-5-4
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-5-5
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:51 /test/2020-05-25--00/part-5-6
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-6-0
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-6-1
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-6-2
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-6-3
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-6-4
 * -rw-r--r--   3 zhushang supergroup         24 2020-05-25 00:50 /test/2020-05-25--00/part-6-5
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:51 /test/2020-05-25--00/part-6-6
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-7-0
 * -rw-r--r--   3 zhushang supergroup         24 2020-05-25 00:50 /test/2020-05-25--00/part-7-1
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-7-2
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-7-3
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:50 /test/2020-05-25--00/part-7-4
 * -rw-r--r--   3 zhushang supergroup         24 2020-05-25 00:50 /test/2020-05-25--00/part-7-5
 * -rw-r--r--   3 zhushang supergroup         12 2020-05-25 00:51 /test/2020-05-25--00/part-7-6
 *
 * hadoop fs -cat /test/2020-05-25--00/part-0-0
 * 20/05/25 00:52:22 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using
 * builtin-java classes where applicable
 * sdfsf
 * ttttt
 */
    }
}
