package window.assigner;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import window.datasource.SourceGenerator;
import window.function.ProcessingWindowFunction;

/**
 * session窗口，固定（动态）间隔内没有数据，则任务前一个窗口结束
 * 由于session window没有固定间隔，所以会为每一条数据分配一个窗口，如果两条数据的时间间隔小于Gap则会合并为一个窗口，
 * 同时由于窗口会合并，所以触发器、窗口函数也需要支持合并
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html#session-windows
 */
public class SessionWindowAssigner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src = SourceGenerator.fromElements(env);
        //允许固定时间的gap
        //src.keyBy(0).window(EventTimeSessionWindows.withGap(Time.seconds(2)))
        //        .process(new ProcessingWindowFunction())
        //        .print();
        //动态调整gap大小
        src.keyBy(t->t.f0).window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple3<String, Integer, Long>>() {
            @Override
            public long extract(Tuple3<String, Integer, Long> element) {
                return element.f1*1000;
            }
        })).process(new ProcessingWindowFunction()).print();
        env.execute();
    }
}
