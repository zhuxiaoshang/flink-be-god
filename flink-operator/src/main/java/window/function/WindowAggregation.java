package window.function;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import window.datasource.SourceGenerator;

/**
 * 结合窗口的聚合demo,聚合操作发生在触发窗口计算时才会取做计算
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html#aggregatefunction
 */
public class WindowAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src = SourceGenerator.fromElements(env);
        src.keyBy(t->t.f0).window(TumblingEventTimeWindows.of(Time.seconds(3))).aggregate(new AggregateWindowFunction()).print();
        env.execute();
    }
}
