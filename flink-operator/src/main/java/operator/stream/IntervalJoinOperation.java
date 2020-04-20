package operator.stream;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * intervalJoin算子，相同key，右流在左流一定时间区间内则可以匹配上，不受固定大小的窗口的限制
 * KeyedStream,KeyedStream → DataStream
 */
public class IntervalJoinOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src1 = env.fromElements(Tuple3.of("k1", 11, 1587314572000L), Tuple3.of("k2", 2, 1587314574000L)
                , Tuple3.of("k3", 3, 1587314576000L), Tuple3.of("k2", 5, 1587314575000L)
                , Tuple3.of("k3", 10, 1587314577000L), Tuple3.of("k1", 9, 1587314579000L)).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>>() {
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
        });
        DataStream<Tuple3<String, Integer, Long>> src2 = env.fromElements(Tuple3.of("k4", 11, 1587314572000L),
                Tuple3.of("k5", 2, 1587314574000L), Tuple3.of("k1", 7, 1587314587000L), Tuple3.of("k2", 12,
                        1587314570000L)).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>>() {
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
        });
        // key1 == key2 && leftTs - 5 < rightTs < leftTs + 5
        src1.keyBy(0).intervalJoin(src2.keyBy(0)).between(Time.seconds(-5), Time.seconds(5))
                //.upperBoundExclusive()//可选，去除上界
                //.lowerBoundExclusive()//可选，去除下界
                .process(new ProcessJoinFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Object>() {
                    @Override
                    public void processElement(Tuple3<String, Integer, Long> left, Tuple3<String, Integer, Long> right, Context ctx, Collector<Object> out) throws Exception {
                        System.out.println(Tuple4.of(left.f0, left.f1, right.f0, right.f1));
                        out.collect(Tuple4.of(left.f0, left.f1, right.f0, right.f1));
                    }
                }).print();
        env.execute();

    }
}
