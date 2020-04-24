package window.evictor;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import window.datasource.SourceGenerator;
import window.function.ApplyAllWindowFunction;

/**
 * 驱逐器evictor，可以在窗口触发前后清除部分元素
 * 基于时间的evitor
 */
public class TimeEvictorOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src = SourceGenerator.fromElements(env);
        //TimeEvictor默认是evictBefore
        //如果需要evictAfter,TimeEvictor.of(Time.seconds(5),true)
        src.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).evictor(TimeEvictor.of(Time.seconds(5), false)).apply(new ApplyAllWindowFunction()).print();
        env.execute();
    }
}
