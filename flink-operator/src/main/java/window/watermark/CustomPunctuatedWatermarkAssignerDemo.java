package window.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPunctuatedWatermarksAdapter;
import window.datasource.SourceGenerator;
import window.function.ApplyWindowFunction;

import javax.annotation.Nullable;

/**
 * 根据元素生成水印，
 */
public class CustomPunctuatedWatermarkAssignerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src =
                env.addSource(new SourceGenerator()).setParallelism(1)
                        .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(
                                new AssignerWithPunctuatedWatermarksAdapter.Strategy<>(
                                        new AssignerWithPunctuatedWatermarks<Tuple3<String, Integer, Long>>() {
                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(Tuple3<String, Integer, Long> lastElement, long extractedTimestamp) {
                                return new Watermark(lastElement.f2>extractedTimestamp?lastElement.f2:extractedTimestamp);
                            }

                            @Override
                            public long extractTimestamp(Tuple3<String, Integer, Long> element, long previousElementTimestamp) {
                                return element.f2;
                            }
                        })))
                        /*.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple3<String, Integer, Long>>() {
                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(Tuple3<String, Integer, Long> lastElement, long extractedTimestamp) {
                        return new Watermark(lastElement.f2>extractedTimestamp?lastElement.f2:extractedTimestamp);
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element, long previousElementTimestamp) {
                        return element.f2;
                    }
                })*/
                ;
        src.keyBy(t->t.f0).timeWindow(Time.seconds(5)).apply(new ApplyWindowFunction()).print();
        env.execute();
    }
}
