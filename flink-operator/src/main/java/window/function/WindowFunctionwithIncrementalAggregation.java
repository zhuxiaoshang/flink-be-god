package window.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import window.datasource.SourceGenerator;

/**
 * 增量聚合的窗口函数，随着窗口数据的不断流入，一直在做聚合，在窗口结束时触发窗口函数的计算，并将聚合值作为窗户函数的输入
 * 这个功能使用场景比较多，比如：没一小时统计当天的累计GMV，或者每分钟统计当天商品销量的topN
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html#processwindowfunction-with-incremental-aggregation
 * reduce/fold 与aggregate类似
 */
public class WindowFunctionwithIncrementalAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src = SourceGenerator.fromElements(env);
        /**
         * 定义1分钟的窗口，实时聚合操作，每5秒钟触发一次窗口计算，比如输出聚合值
         */
        src.keyBy(t -> t.f0).timeWindow(Time.minutes(1)).trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
                .aggregate(new AggregateWindowFunction(), new ProcessWindowFunction<Integer, Object, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Integer> elements, Collector<Object> out) throws Exception {
                        //do something
                        //可以获取状态
                        //https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html#using-per-window-state-in-processwindowfunction
                        //context.windowState();
                        //context.globalState();
                        System.out.println("窗口开始时间=" + context.window().getStart() + ",结束时间=" + context.window().getEnd());
                        System.out.println("key = " + key);
                        System.out.println("窗口内数据=" + elements);
                    }
                }).print();
        env.execute();
    }
}
