package window.datasource;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class SourceGenerator extends RichParallelSourceFunction<Tuple3<String, Integer, Long>> {
    public static DataStream fromElements(StreamExecutionEnvironment env) {
        DataStream<Tuple3<String, Integer, Long>> src = env.fromElements(Tuple3.of("k1", 11, 1587314572000L), Tuple3.of("k2", 2, 1587314574000L)
                , Tuple3.of("k3", 3, 1587314576000L), Tuple3.of("k2", 5, 1587314575000L)
                , Tuple3.of("k3", 10, 1587314577000L), Tuple3.of("k1", 9, 1587314579000L)).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>>() {
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
        return src;
    }

    @Override
    public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
        Tuple3<String, Integer, Long>[] elements = new Tuple3[]{Tuple3.of("k1", 6, 1587314571000L),
                Tuple3.of("k2", 2, 1587314574000L), Tuple3.of("k4", 8, 1587314573000L)
                , Tuple3.of("k3", 3, 1587314576000L), Tuple3.of("k2", 5, 1587314575000L), Tuple3.of("k1", 9, 1587314572000L)
                , Tuple3.of("k3", 13, 1587314577000L), Tuple3.of("k1", 15, 1587314582000L),Tuple3.of("k2",1,
                1587314579000L)
                , Tuple3.of("k4", 20, 1587314581000L)
        };
        for (Tuple3 t : elements
        ) {
            ctx.collect(t);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}
