package window.trigger;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import window.datasource.SourceGenerator;
import window.function.ApplyWindowFunction;

/**
 * 基于process time的窗口，默认具有ProcessingTimeTrigger,当系统时间到达窗口结束时间触发窗口计算
 * @see{@link org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger}
 */
public class ProcessingTimeTriggerOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<Tuple3<String, Integer, Long>> src = env.addSource(new SourceGenerator()).setParallelism(1);
        src.keyBy(t->t.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(3))).apply(new ApplyWindowFunction()).print();
        env.execute();
    }
}
