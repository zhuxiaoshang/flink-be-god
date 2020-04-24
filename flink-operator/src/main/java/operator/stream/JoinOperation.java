package operator.stream;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import javax.annotation.Nullable;

/**
 * join算子，双流 inner join只有两边都有的数据才能关联上，只能基于划分的窗口内的join
 * DataStream,DataStream → DataStream
 */
public class JoinOperation
{
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
        DataStream<Tuple4<String, Integer, Long,String>> src2 = env.fromElements(Tuple4.of("k4", 11, 1587314572000L,
                "sdfs"),
                Tuple4.of("k5", 2, 1587314574000L,"11fsd"),Tuple4.of("k1", 7, 1587314573000L,"tttt"),Tuple4.of("k2", 12,
                        1587314571000L,"adfsdf")).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple4<String, Integer, Long,String>>() {
            long currentTimeStamp;
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentTimeStamp);
            }

            @Override
            public long extractTimestamp(Tuple4<String, Integer, Long,String> element, long previousElementTimestamp) {
                currentTimeStamp = Math.max(currentTimeStamp, element.f2);
                return element.f2;
            }
        });
        src1.join(src2).where(t->t.f0).equalTo(t->t.f0).window(TumblingEventTimeWindows.of(Time.seconds(5))).apply(new JoinFunction<Tuple3<String, Integer, Long>, Tuple4<String, Integer, Long, String>, Object>() {
            @Override
            public Object join(Tuple3<String, Integer, Long> first, Tuple4<String, Integer, Long, String> second) throws Exception {
                return Tuple4.of(first.f0,first.f1,second.f0,second.f1);
            }
        }).print();
        env.execute();

    }

}
