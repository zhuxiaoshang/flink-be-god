package connector.filesystem;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.sequencefile.SequenceFileWriterFactory;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;


/**
 * 写入hdfs，
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
//        BucketingSink<String> sink = new BucketingSink<String>("hdfs://namenode:8020/tmp/zs");
//        sink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd--HHmm", ZoneId.of("Asia/Shanghai")));
//        sink.setWriter(new StringWriter<>());
//        sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,
//        sink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins


//        Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
        map.addSink(StreamingFileSink.forRowFormat(new Path("hdfs://namenode:8020/tmp/zs"), new SimpleStringEncoder<String>("UTF-8")).build());

//        map.addSink(StreamingFileSink.forBulkFormat(new Path("hdfs://namenode:8020/tmp/zs"),
//                new SequenceFileWriterFactory(hadoopConf, String.class, String.class)).withBucketCheckInterval(1000L)
//                //滚动策略，依赖checkpoint，完成checkpoint后生成新文件，格式part-subtask-fileindex,可以设置前缀后缀
//                .withRollingPolicy(OnCheckpointRollingPolicy.build())
//                .build()).setParallelism(1);
        env.execute();

    }
}
