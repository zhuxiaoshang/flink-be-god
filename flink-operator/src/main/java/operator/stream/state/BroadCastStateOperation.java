package operator.stream.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * no-keyed broadcast state
 */
public class BroadCastStateOperation {
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
        normalStream.connect(broadcast).process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple2<String, String>, Object>() {
            @Override
            public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<Object> out) throws Exception {
                System.out.println(Thread.currentThread().getName()+"element from normal stream :"+value);
                //这里状态是只读的
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(descriptor);
                if(broadcastState.contains(value.f0)){
                    out.collect(Tuple3.of(value.f0,value.f1,broadcastState.get(value.f0)));
                }
            }

            @Override
            public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<Object> out) throws Exception {
                System.out.println(Thread.currentThread().getName()+"element from broadcast stream :"+value);
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(descriptor);
                //将广播流数据存入状态
                broadcastState.put(value.f0,value.f1);
            }
        }).setParallelism(4).print();
        env.execute();

    }
}
