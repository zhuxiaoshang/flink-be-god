package window.trigger;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 在整理【实时数仓篇】Flink 窗口的应用与实现 时，对其中Trigger示例有疑问做的验证
 * https://www.bilibili.com/video/BV1n54y1976u/
 */
public class ContinuousEventTimeTriggerOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStreamSource<String> src = env.addSource(new RichParallelSourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                String[] strs = {"1589025600000,1","1589025660000,2","1589025720000,3","1589025780000,2"};
                for (String s:strs
                     ) {
                    ctx.collect(s);
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {

            }
        });
        src.flatMap(new FlatMapFunction<String, Tuple2<Long,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<Long,Integer>> collector) throws Exception {
                String[] strings = s.split(",");
                collector.collect(Tuple2.of(Long.parseLong(strings[0]),Integer.parseInt(strings[1])));
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner((e,t)->e.f0))
               /* .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<Long, Integer>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple2<Long, Integer> element) {
                return element.f0;
            }
        })*/
                .timeWindowAll(Time.minutes(5)).trigger(ContinuousEventTimeTrigger.of(Time.minutes(2))).sum(1)
                .print();
        env.execute();
    }
}
