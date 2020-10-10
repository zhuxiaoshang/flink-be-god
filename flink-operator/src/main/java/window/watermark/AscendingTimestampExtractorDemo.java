package window.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import window.datasource.SourceGenerator;
import window.function.ApplyWindowFunction;

/**
 * 严格递增的水印生成器，可支持违反递增规则数据的处理withViolationHandler，默认是打日志的方式，还支持抛异常和丢弃两种处理
 * 适用场景：适用于elements的时间在每个parallel task里头是单调递增(timestamp monotony)的场景
 */
public class AscendingTimestampExtractorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src =
                env.addSource(new SourceGenerator()).setParallelism(1);
        /**
         * AscendingTimestampExtractor已废弃使用新版本的WatermarkStrategy
        src.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Integer, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple3<String, Integer, Long> element) {
                        return element.f2;
                    }
                    //抛异常处理，默认打日志
                }.withViolationHandler(new AscendingTimestampExtractor.FailingHandler()));
         */
        src.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forMonotonousTimestamps().withTimestampAssigner((e,t)->e.f2));
        src.keyBy(t->t.f0).timeWindow(Time.seconds(5))
                .apply(new ApplyWindowFunction()).print();
        env.execute();
    }
}
