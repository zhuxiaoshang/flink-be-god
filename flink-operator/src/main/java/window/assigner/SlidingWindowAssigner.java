package window.assigner;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import window.datasource.SourceGenerator;
import window.function.ProcessingWindowFunction;

/**
 * slide滑动窗口，允许固定大小+滑动间隔，窗口间可能有重叠或间隔，窗口大小=滑动间隔时相当于tumble窗口
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html#sliding-windows
 */
public class SlidingWindowAssigner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src = SourceGenerator.fromElements(env);
        src.keyBy(t->t.f0).window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1))).process(new ProcessingWindowFunction()).print();
        env.execute();
    }
}
