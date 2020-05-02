package connector.filesystem;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;


/**
 * 读取本地文件，处理后writeAsText写入本地文件
 */
public class FileConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        DataStream<String> src = env.readTextFile("/Users/zhushang/Desktop/software/1.txt");
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
         * 已废弃的方法，建议用addSink(StreamingFileSink)代替
         * Writes a DataStream to the file specified by path in text format.
         *
         * <p>For every element of the DataStream the result of {@link Object#toString()} is written.
         *
         * @param path
         *            The path pointing to the location the text file is written to.
         *
         * @return The closed DataStream.
         *
         * @deprecated Please use the {@link org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink} explicitly using the
         * {@link #addSink(SinkFunction)} method.
         */
        map.writeAsText("/Users/zhushang/Desktop/software/output.txt").setParallelism(1);

        /**
         * 开始并行度没有设置默认是cpu核数，所以output.txt作为了目录名，每个并发对应一个文件
         * -rw-r--r--  1 zhushang  staff   92  5  1 17:32 1
         * -rw-r--r--  1 zhushang  staff   54  5  1 17:32 2
         * -rw-r--r--  1 zhushang  staff   42  5  1 17:32 3
         * -rw-r--r--  1 zhushang  staff   98  5  1 17:32 4
         * -rw-r--r--  1 zhushang  staff   96  5  1 17:32 5
         * -rw-r--r--  1 zhushang  staff   91  5  1 17:32 6
         * -rw-r--r--  1 zhushang  staff  146  5  1 17:32 7
         * -rw-r--r--  1 zhushang  staff   99  5  1 17:32 8
         *
         * 改成1个并发后，输出output.txt文件
         * -rw-r--r--@  1 zhushang  staff   718  5  1 17:35 output.txt
         */
        env.execute();
    }
}
