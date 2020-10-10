package window.assigner;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import window.datasource.SourceGenerator;

/**
 * 连续窗口计算
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html#consecutive-windowed-operations
 */
public class ConsecutiveWindowedOperation {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src =
                env.addSource(new SourceGenerator()).setParallelism(1).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, Long>>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element) {
                        return element.f2;
                    }
                });
        //每3秒统计TOP1
        src.keyBy(t->t.f0).timeWindow(Time.seconds(3)).sum(1).timeWindowAll(Time.seconds(3)).max(1).print();
        env.execute();

    }
}
