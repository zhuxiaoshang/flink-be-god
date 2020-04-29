package operator.stream;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedProcessOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of("k1", 1), Tuple2.of("k2", 2), Tuple2.of("k3", 3), Tuple2.of("k2", 5))
                .keyBy(0)//可以用0或t->t.f0
                .process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Object>() {
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Object> out) throws Exception {

                    }
                });
        env.execute();
    }
}
