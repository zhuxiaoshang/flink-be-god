package window.watermark;

import com.sun.org.apache.bcel.internal.generic.F2D;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import window.datasource.SourceGenerator;
import window.function.ApplyWindowFunction;

import javax.annotation.Nullable;
import java.time.Duration;

/**
 * 自定义周期水印生成
 */
public class CustomPeriodicWatermarkAssignerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src =
                env.addSource(new SourceGenerator()).setParallelism(1)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                        .withTimestampAssigner((e,t)->e.f2))
                        /*.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>>() {
                    long currentTimestamp;
                    final long DELAY = 3L * 1000; //3s延迟
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimestamp-DELAY);
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element, long previousElementTimestamp) {
                        currentTimestamp = Math.max(currentTimestamp, element.f2);
                        return element.f2;
                    }
                })*/
                ;
        src.keyBy(t->t.f0).timeWindow(Time.seconds(5)).apply(new ApplyWindowFunction()).print();
        env.execute();
    }
}
