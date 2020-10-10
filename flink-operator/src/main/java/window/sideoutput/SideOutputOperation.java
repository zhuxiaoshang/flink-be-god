package window.sideoutput;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import window.datasource.SourceGenerator;
import window.function.ApplyAllWindowFunction;

import java.time.Duration;

/**
 * 将窗口延迟的数据通过side output输出
 */
public class SideOutputOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src =
                env.addSource(new SourceGenerator()).setParallelism(1)
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner((e, t) -> e.f2))
                        /*.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, Long>>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element) {
                        return element.f2;
                    }
                })*/;
        OutputTag<Tuple3<String, Integer, Long>> lateData = new OutputTag<Tuple3<String, Integer, Long>>("late-data") {
        };

        SingleOutputStreamOperator<Object> result =
                src.timeWindowAll(Time.seconds(3)).sideOutputLateData(lateData).apply(new ApplyAllWindowFunction());
        //result.print();
        DataStream<Tuple3<String, Integer, Long>> sideOutput = result.getSideOutput(lateData);
        //输出被丢弃的数据
        sideOutput.print();
        env.execute();
    }
}
