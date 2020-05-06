package operator.stream;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import window.datasource.SourceGenerator;

/**
 * keyed处理函数，可以定义状态和定时器
 */
public class KeyedProcessOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, Integer, Long>> src = env.addSource(new SourceGenerator()).setParallelism(1);
        src.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, Long>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple3<String, Integer, Long> element) {
                return element.f2;
            }
        }).keyBy(0)//可以用0或t->t.f0
                .process(new KeyedProcessFunction<Tuple, Tuple3<String, Integer, Long>, Object>() {
                    ListState<Tuple3<String, Integer, Long>> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, Integer, Long>>("list"
                        , TypeInformation.of(new TypeHint<Tuple3<String, Integer, Long>>() {
                                })));
                    }

                    /**
                     * Called when a timer set using {@link TimerService} fires.
                     *
                     * @param timestamp The timestamp of the firing timer.
                     * @param ctx       An {@link OnTimerContext} that allows querying the timestamp, the {@link TimeDomain}, and the key
                     *                  of the firing timer and getting a {@link TimerService} for registering timers and querying the time.
                     *                  The context is only valid during the invocation of this method, do not store it.
                     * @param out       The collector for returning result values.
                     * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
                     *                   to fail and may trigger recovery.
                     */
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                        System.out.println("定时器触发，timestamp="+timestamp+",current key = "+ctx.getCurrentKey()+",");
                        listState.clear();
                    }

                    @Override
                    public void processElement(Tuple3<String, Integer, Long> value, Context ctx, Collector<Object> out) throws Exception {
                        System.out.println("当前元素："+value+",timestamp="+ctx.timestamp());
                        listState.add(value);
                        ctx.timerService().registerEventTimeTimer(value.f2+5000L);
                    }
                }).print();
        env.execute();
    }
}
