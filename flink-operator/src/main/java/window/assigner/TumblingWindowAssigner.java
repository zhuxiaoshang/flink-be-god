package window.assigner;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import window.datasource.SourceGenerator;
import window.function.ApplyWindowFunction;

/**
 * tumble滚动窗口，固定大小的窗口，窗口之间不会有重合，窗口前闭后开[)
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html#tumbling-windows
 */
public class TumblingWindowAssigner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src = SourceGenerator.fromElements(env);
        //window的offset默认可以不写，天窗口可以指定offset=-8 hours，从0点开始
        src.keyBy(t -> t.f0).window(TumblingEventTimeWindows.of(Time.seconds(3), Time.seconds(0))).apply(new ApplyWindowFunction()).print();
        env.execute();
    }
}
