package operator.stream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * window算子，对keyed流数据按一定的时间属性划分窗口,相同的key会被分配到同一个subtask，目前有TumbleWindow/SlideWindow/SessionWindow/GlobleWindow
 * /CountWindow
 * 这里只是展示window算子的用法，后面会有demo对window做详细说明
 * KeyedStream → WindowedStream
 */
public class WindowOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.fromElements(Tuple3.of("k1", 11, 1587314572000L), Tuple3.of("k2", 2, 1587314574000L)
                , Tuple3.of("k3", 3, 1587314576000L), Tuple3.of("k2", 5, 1587314575000L)
                , Tuple3.of("k3", 10, 1587314577000L), Tuple3.of("k1", 9, 1587314579000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((e, t) -> e.f2))
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
                .keyBy(t -> t.f0)//可以用0或t->t.f0
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<Tuple3<String, Integer, Long>, Object, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple3<String, Integer, Long>> input, Collector<Object> out) throws Exception {
                        System.out.println("窗口开始时间=" + window.getStart() + ",结束时间=" + window.getEnd());
                        System.out.println("key = " + key);
                        System.out.println("窗口内数据=" + input);
                    }
                })
                .print();
        env.execute();
    }
}
