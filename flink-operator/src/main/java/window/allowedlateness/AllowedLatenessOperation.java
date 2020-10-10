package window.allowedlateness;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import window.datasource.SourceGenerator;
import window.function.ApplyAllWindowFunction;

import java.time.Duration;

/**
 * 允许延迟时间,在水印延迟的基础上再容忍一定延迟数据的处理
 * 比如，水印延迟2s，allowedlateness延迟3s，则延迟2s内的数据正常落在窗口内处理，2s-3s的数据则会每来一条触发一次窗口处理
 * 由于重复计算，所以下游需要做去重处理
 * 如果水印延迟>=allowedlateness,则allowedlateness不会起作用
 */
public class AllowedLatenessOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src =
                env.addSource(new SourceGenerator()).setParallelism(1)
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner((e,t)->e.f2))
                      /*  .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element) {
                        return element.f2;
                    }
                })*/
                ;
        src.timeWindowAll(Time.seconds(5)).allowedLateness(Time.seconds(2)).apply(new ApplyAllWindowFunction()).print();
        env.execute();
        /**
         * 水印延迟0s时
         * 窗口开始时间=1587314570000,结束时间=1587314575000
         * 窗口内数据=[(k2,2,1587314574000)]
         * 3> (k2,2,1587314574000)
         * 窗口开始时间=1587314570000,结束时间=1587314575000
         * 窗口内数据=[(k2,2,1587314574000), (k1,9,1587314572000)]
         * 4> (k2,2,1587314574000)
         * 1> (k1,9,1587314572000)
         * 窗口开始时间=1587314575000,结束时间=1587314580000
         * 窗口内数据=[(k3,3,1587314576000), (k2,5,1587314575000), (k3,13,1587314577000)]
         * 3> (k2,5,1587314575000)
         * 4> (k3,13,1587314577000)
         * 2> (k3,3,1587314576000)
         * 窗口开始时间=1587314580000,结束时间=1587314585000
         * 窗口内数据=[(k1,15,1587314582000), (k4,20,1587314581000)]
         * 2> (k4,20,1587314581000)
         * 1> (k1,15,1587314582000)
         * ==================
         *水印延迟2s时
         * 窗口开始时间=1587314570000,结束时间=1587314575000
         * 窗口内数据=[(k2,2,1587314574000), (k1,9,1587314572000)]
         * 4> (k1,9,1587314572000)
         * 3> (k2,2,1587314574000)
         * 窗口开始时间=1587314575000,结束时间=1587314580000
         * 窗口内数据=[(k3,3,1587314576000), (k2,5,1587314575000), (k3,13,1587314577000)]
         * 2> (k2,5,1587314575000)
         * 1> (k3,3,1587314576000)
         * 3> (k3,13,1587314577000)
         * 窗口开始时间=1587314580000,结束时间=1587314585000
         * 窗口内数据=[(k1,15,1587314582000), (k4,20,1587314581000)]
         * 4> (k1,15,1587314582000)
         * 1> (k4,20,1587314581000)
         */

    }
}
