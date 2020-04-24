package window.trigger;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import window.datasource.SourceGenerator;
import window.function.AggregateWindowFunction;

/**
 * PurgingTrigger配合其他触发器使用，可以在其他触发器触发计算后将窗口状态清除
 */
public class PurgingTriggerOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<Tuple3<String, Integer, Long>> src = env.addSource(new SourceGenerator()).setParallelism(1);
        src.timeWindowAll(Time.seconds(10)).trigger(PurgingTrigger.of(ContinuousProcessingTimeTrigger.of(Time.seconds(3)))).aggregate(new AggregateWindowFunction(), new ProcessAllWindowFunction<Integer, Object, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Integer> elements, Collector<Object> out) throws Exception {
                System.out.println("窗口开始时间=" + context.window().getStart() + ",结束时间=" + context.window().getEnd());
                System.out.println("窗口内数据=" + elements);
            }
        }).print();
        env.execute();
    }
}
