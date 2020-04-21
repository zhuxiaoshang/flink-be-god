package window.assign;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import window.datasource.SourceGenerator;

/**
 * globle窗口，把所有数据都放一个窗口内处理，需要自定义触发器来触发计算
 * CountWindow就是用的GlobalWindow实现
 */
public class GlobleWindowAssigner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src = SourceGenerator.fromElements(env);
        src.keyBy(0).window(GlobalWindows.create()).trigger(CountTrigger.of(2)).process(new ProcessWindowFunction<Tuple3<String, Integer, Long>, Object, Tuple, GlobalWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple3<String, Integer, Long>> elements, Collector<Object> out) throws Exception {
                System.out.println("key = " + tuple);
                System.out.println("窗口内数据=" + elements);
            }
        }).print();
        env.execute();
    }
}
