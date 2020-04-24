package window.watermark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.planner.plan.nodes.physical.stream.PeriodicWatermarkAssignerWrapper;
import window.datasource.SourceGenerator;
import window.function.ApplyWindowFunction;

public class PeriodicWatermarkAssignerWrapperDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//这里时间语义不起作用
        DataStream<Tuple3<String, Integer, Long>> src =
                env.addSource(new SourceGenerator()).setParallelism(1).assignTimestampsAndWatermarks(new PeriodicWatermarkAssignerWrapper());
        src.keyBy(0).timeWindow(Time.seconds(5)).apply(new ApplyWindowFunction()).print();
        env.execute();
    }
}
