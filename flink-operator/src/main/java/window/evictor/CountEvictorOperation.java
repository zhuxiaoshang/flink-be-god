package window.evictor;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import window.datasource.SourceGenerator;
import window.function.ApplyAllWindowFunction;

/**
 *
 * 基于数量的evitor
 */
public class CountEvictorOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src = SourceGenerator.fromElements(env);
        src.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).evictor(CountEvictor.of(4,true)).apply(new ApplyAllWindowFunction()).print();
        env.execute();
    }
}
