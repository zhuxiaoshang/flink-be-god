package window.evictor;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import window.datasource.SourceGenerator;
import window.function.ApplyAllWindowFunction;

import java.util.Iterator;

/**
 *
 * 自定义evitor
 */
public class CustomEvictorOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src = SourceGenerator.fromElements(env);
        src.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).evictor(new Evictor<Tuple3<String, Integer, Long>, TimeWindow>() {
            @Override
            public void evictBefore(Iterable<TimestampedValue<Tuple3<String, Integer, Long>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
                for(Iterator<TimestampedValue<Tuple3<String, Integer, Long>>> iterator = elements.iterator();iterator.hasNext();){
                    TimestampedValue<Tuple3<String, Integer, Long>> next = iterator.next();
                    Tuple3<String, Integer, Long> value = next.getStreamRecord().getValue();
                    if(value.f1==5){
                        iterator.remove();
                    }
                }
            }

            @Override
            public void evictAfter(Iterable<TimestampedValue<Tuple3<String, Integer, Long>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
                for(Iterator<TimestampedValue<Tuple3<String, Integer, Long>>> iterator = elements.iterator();iterator.hasNext();){
                    TimestampedValue<Tuple3<String, Integer, Long>> next = iterator.next();
                    Tuple3<String, Integer, Long> value = next.getStreamRecord().getValue();
                    if(value.f1==9){
                        iterator.remove();
                    }
                }
            }
        }).apply(new ApplyAllWindowFunction()).print();
        env.execute();
    }
}
