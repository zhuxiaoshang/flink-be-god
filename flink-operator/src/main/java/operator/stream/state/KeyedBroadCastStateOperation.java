package operator.stream.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 基于key的广播流关联，和none-keyed类似，多了定时器功能
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/state/broadcast_state.html#broadcastprocessfunction-and-keyedbroadcastprocessfunction
 *
 */
public class KeyedBroadCastStateOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, String>> normalStream =
                env.addSource(new RichParallelSourceFunction<Tuple2<String, String>>() {
                    @Override
                    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
                        Random random = new Random();
                        while (true) {
                            ctx.collect(Tuple2.of(random.nextInt(10) + "", "this normal stream"));
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                }).setParallelism(4);
        DataStreamSource<Tuple2<String, String>> broadCastStream =
                env.addSource(new RichSourceFunction<Tuple2<String, String>>() {
                    @Override
                    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
                        Random random = new Random();
                        while (true) {
                            ctx.collect(Tuple2.of(random.nextInt(10) + "", "this broadcast stream"));
                            Thread.sleep(5000L);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                }).setParallelism(1);
        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>("BroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        BroadcastStream<Tuple2<String, String>> broadcast = broadCastStream.broadcast(descriptor);
        normalStream.keyBy(0).connect(broadcast).process(new KeyedBroadcastProcessFunction<Object, Tuple2<String, String>, Tuple2<String, String>, Object>() {
            @Override
            public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<Object> out) throws Exception {
                System.out.println(Thread.currentThread().getName()+"element from normal stream :"+value);
                //这里状态是只读的
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(descriptor);
                if(broadcastState.contains(value.f0)){
                    out.collect(Tuple3.of(value.f0,value.f1,broadcastState.get(value.f0)));
                }
                //如果非广播流的数据也需要state存储，则需要另外定义状态

                ctx.timerService().registerProcessingTimeTimer(ctx.currentProcessingTime()+10000L);
            }

            @Override
            public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<Object> out) throws Exception {
                System.out.println(Thread.currentThread().getName()+"element from broadcast stream :"+value);
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(descriptor);
                //将广播流数据存入状态
                broadcastState.put(value.f0,value.f1);
            }

            /**
             * Called when a timer set using {@link TimerService} fires.
             *
             * @param timestamp The timestamp of the firing timer.
             * @param ctx       An {@link OnTimerContext} that allows querying the timestamp of the firing timer,
             *                  querying the current processing/event time, iterating the broadcast state
             *                  with <b>read-only</b> access, querying the {@link TimeDomain} of the firing timer
             *                  and getting a {@link TimerService} for registering timers and querying the time.
             *                  The context is only valid during the invocation of this method, do not store it.
             * @param out       The collector for returning result values.
             * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
             *                   to fail and may trigger recovery.
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                System.out.println("onTimer execute!");
            }
        }).setParallelism(4).print();
        env.execute();
    }
}
