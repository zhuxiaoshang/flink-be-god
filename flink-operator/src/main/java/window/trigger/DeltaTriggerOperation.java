package window.trigger;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.NumberSerializers;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import window.datasource.SourceGenerator;
import window.function.ApplyAllWindowFunction;
import window.function.ApplyWindowFunction;

import javax.annotation.Nullable;

/**
 * delta触发器，给定一个阈值，在当前元素和上次触发计算元素减的差值超过阈值则触发计算
 */
public class DeltaTriggerOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src = env.addSource(new SourceGenerator()).setParallelism(1).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>>() {
            long currentTimeStamp;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentTimeStamp);
            }

            @Override
            public long extractTimestamp(Tuple3<String, Integer, Long> element, long previousElementTimestamp) {
                currentTimeStamp = Math.max(currentTimeStamp, element.f2);
                return element.f2;
            }
        });
        src.timeWindowAll(Time.seconds(20)).trigger(PurgingTrigger.of(DeltaTrigger.of(5, new DeltaFunction<Tuple3<String, Integer, Long>>() {
            @Override
            public double getDelta(Tuple3<String, Integer, Long> oldDataPoint, Tuple3<String, Integer, Long> newDataPoint) {
                return newDataPoint.f1 - oldDataPoint.f1;
            }
        }, src.getType().createSerializer(env.getConfig())))).apply(new ApplyAllWindowFunction()).print();
        env.execute();
        /**
         * 窗口开始时间=1587314560000,结束时间=1587314580000
         * 窗口内数据=[(k2,2,1587314574000), (k3,3,1587314576000), (k2,5,1587314575000), (k1,9,1587314572000)]
         * 窗口开始时间=1587314560000,结束时间=1587314580000
         * 窗口内数据=[(k3,13,1587314577000), (k1,15,1587314579000)]
         * 不用PurgingTrigger,结果是
         * 窗口开始时间=1587314560000,结束时间=1587314580000
         * 窗口内数据=[(k2,2,1587314574000), (k3,3,1587314576000), (k2,5,1587314575000), (k1,9,1587314572000)]
         * 窗口开始时间=1587314560000,结束时间=1587314580000
         * 窗口内数据=[(k2,2,1587314574000), (k3,3,1587314576000), (k2,5,1587314575000), (k1,9,1587314572000),(k3,13,1587314577000), (k1,15,1587314579000)]
         */
    }
}
