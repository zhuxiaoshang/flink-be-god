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
 * addSink方式写入本地文件
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/connectors/streamfile_sink.html
 */
public class FileSinkConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        DataStream<String> src = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (true) {
                    ctx.collect("sdfsf fdsfsf");
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
        map.addSink(StreamingFileSink.forRowFormat(new Path("/Users/zhushang/Desktop/software/"),
                new SimpleStringEncoder<String>("UTF-8")).withBucketCheckInterval(1000L)
                //滚动策略，依赖checkpoint，完成checkpoint后生成新文件，格式part-subtask-fileindex,可以设置前缀后缀
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build()).setParallelism(1);
        env.execute();
        /**
         * 默认一个小时生成一个文件夹 格式：2020-05-01--17
         * zhushangdeMacBook-Pro:2020-05-01--17 zhushang$ ls -l
         * total 56
         * -rw-r--r--  1 zhushang  staff   65  5  1 17:50 part-0-0
         * -rw-r--r--  1 zhushang  staff  130  5  1 17:50 part-0-1
         * -rw-r--r--  1 zhushang  staff  130  5  1 17:50 part-0-2
         * -rw-r--r--  1 zhushang  staff  130  5  1 17:50 part-0-3
         * -rw-r--r--  1 zhushang  staff  130  5  1 17:51 part-0-4
         * -rw-r--r--  1 zhushang  staff  130  5  1 17:51 part-0-5
         * -rw-r--r--  1 zhushang  staff  130  5  1 17:51 part-0-6
         */
    }
}
