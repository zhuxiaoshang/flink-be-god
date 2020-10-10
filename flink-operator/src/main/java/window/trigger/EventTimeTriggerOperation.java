package window.trigger;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import window.datasource.SourceGenerator;
import window.function.ApplyWindowFunction;

/**
 * 基于event time的窗口默认具有EventTimeTrigger，即水印时间到达窗口结束时间时触发窗口计算
 * @see{@link EventTimeTrigger}
 */
public class EventTimeTriggerOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src = SourceGenerator.fromElements(env);
        src.keyBy(t->t.f0).window(TumblingEventTimeWindows.of(Time.seconds(3))).apply(new ApplyWindowFunction()).print();
        env.execute();
    }
}
