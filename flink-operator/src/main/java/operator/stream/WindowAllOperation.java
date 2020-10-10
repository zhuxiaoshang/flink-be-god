package operator.stream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.time.Duration;

/**
 * windowAll算子+apply算子，所有数据都会汇聚到一个窗口实例中进行处理，所以并行度为1
 * DataStream → AllWindowedStream → DataStream
 */
public class WindowAllOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.fromElements(Tuple3.of("k1", 11,1587314572000L), Tuple3.of("k2", 2,1587314574000L)
                , Tuple3.of("k3", 3,1587314576000L), Tuple3.of("k2", 5,1587314573000L)
                , Tuple3.of("k3", 10,1587314577000L), Tuple3.of("k1", 9,1587314579000L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((e,t)->e.f2))
                /*.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>>() {
                    long currentTimeStamp;
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimeStamp);
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element, long previousElementTimestamp) {
                        currentTimeStamp = Math.max(currentTimeStamp, element.f2);
                        return element.f2;
                    }
                })*/
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5))).apply(new AllWindowFunction<Tuple3<String, Integer, Long>, Object, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Tuple3<String, Integer, Long>> values, Collector<Object> out) throws Exception {
                System.out.println("窗口开始时间="+window.getStart()+",结束时间="+window.getEnd());
                System.out.println("窗口内数据="+values);
            }
        }).print();
        env.execute();
    }
}
