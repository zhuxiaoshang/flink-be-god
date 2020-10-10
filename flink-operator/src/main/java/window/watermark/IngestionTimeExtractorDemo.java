package window.watermark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import window.datasource.SourceGenerator;
import window.function.ApplyWindowFunction;

/**
 * 基于摄取时间语义的水印生成器,默认获取机器时钟作为水印时间戳。
 */
@Deprecated
public class IngestionTimeExtractorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//这里时间语义不起作用
        DataStream<Tuple3<String, Integer, Long>> src =
                env.addSource(new SourceGenerator()).setParallelism(1).assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());
        src.keyBy(t -> t.f0).timeWindow(Time.seconds(5)).apply(new ApplyWindowFunction()).print();
        env.execute();
    }
}
