package operator.stream;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * cogroup算子，类似于join,join是inner join，cogroup可以实现inner/left/right/full out join
 * 窗口内包含两边流的迭代器
 * DataStream,DataStream → DataStream
 */
public class CoGroupOperation {
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
                Tuple3.of("k5", 2, 1587314574000L), Tuple3.of("k1", 7, 1587314573000L), Tuple3.of("k2", 12,
                        1587314571000L)).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>>() {
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
        src1.coGroup(src2).where(t -> t.f0).equalTo(t -> t.f0).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Object>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<String, Integer, Long>> first, Iterable<Tuple3<String, Integer, Long>> second, Collector<Object> out) throws Exception {
                        System.out.println("first=" + first);
                        System.out.println("second=" + second);
                    }
                }).print();
        env.execute();

    }
}
