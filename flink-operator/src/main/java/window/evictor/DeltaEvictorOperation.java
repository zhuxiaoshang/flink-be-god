package window.evictor;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import window.datasource.SourceGenerator;
import window.function.ApplyAllWindowFunction;

/**
 *
 * 基于变化量的evitor
 */
public class DeltaEvictorOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src = SourceGenerator.fromElements(env);
        src.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).evictor(DeltaEvictor.of(4, new DeltaFunction<Tuple3<String, Integer, Long>>() {
            @Override
            public double getDelta(Tuple3<String, Integer, Long> oldDataPoint, Tuple3<String, Integer, Long> newDataPoint) {
                return newDataPoint.f1-oldDataPoint.f1  ;
            }
        })).apply(new ApplyAllWindowFunction()).print();
        env.execute();
    }
}
