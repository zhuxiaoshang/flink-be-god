package application.traffic_analysis;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.*;
import java.time.format.DateTimeFormatter;

public class TrafficAnalysisMain {
    public static final  DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStreamSource<TrafficFlow> src =
                env.addSource(new ParallelSourceFunction<TrafficFlow>() {
            @Override
            public void run(SourceContext<TrafficFlow> ctx) throws Exception {
                TrafficFlow[] flows = {
                        new TrafficFlow(1, "G50-100", "苏A10000", "2020-06-05 00:00:01"),
                        new TrafficFlow(2, "G50-100", "苏A10001", "2020-06-05 00:02:12"),
                        new TrafficFlow(3, "G50-100", "苏A10002", "2020-06-05 00:12:03"),
                        new TrafficFlow(4, "G50-100", "苏A10003", "2020-06-05 00:22:12"),
                        new TrafficFlow(5, "G50-100", "苏A10004", "2020-06-05 00:30:01"),
                        new TrafficFlow(6, "G50-100", "苏A10005", "2020-06-05 00:31:50"),
                        new TrafficFlow(7, "G50-100", "苏A10006", "2020-06-05 00:36:45"),
                        new TrafficFlow(8, "G50-100", "苏A10007", "2020-06-05 00:45:22"),
                        new TrafficFlow(9, "G50-100", "苏A10008", "2020-06-05 00:55:00"),
                        new TrafficFlow(10, "G50-100", "苏A10009", "2020-06-05 01:06:01"),

                };

                for (TrafficFlow f : flows) {
                    ctx.collect(f);
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {

            }
        });
        src.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TrafficFlow>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(TrafficFlow element) {
                return LocalDateTime.parse(element.getTake_photo_at(),DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toInstant(ZoneOffset.of("+8")).toEpochMilli();
            }
        }).keyBy("device_id").timeWindow(Time.minutes(30)).aggregate(new AggregateFunction<TrafficFlow, Tuple2<String, Integer>,
                Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> createAccumulator() {
                return Tuple2.of("", 0);
            }

            @Override
            public Tuple2<String, Integer> add(TrafficFlow value, Tuple2<String, Integer> accumulator) {
                return Tuple2.of(value.device_id, accumulator.f1 + value.num);
            }

            @Override
            public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                return accumulator;
            }

            @Override
            public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                return Tuple2.of(a.f0, a.f1 + b.f1);
            }
        }, new WindowFunction<Tuple2<String, Integer>, Object, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input,
                              Collector<Object> out) throws Exception {
                Tuple2<String, Integer> agg = input.iterator().next();
                String windowStart = formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(window.getStart()), ZoneId.systemDefault()));
                out.collect(Tuple3.of(agg.f0,windowStart, agg.f1));
            }
        }).print();
        env.execute();
    }
}
