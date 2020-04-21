package operator.stream.partition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * boradcast算子，每一条数据都广播到下游算子所有并行度上
 */
public class BroadcastOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, Long>> src1 = env.fromElements(Tuple3.of("k1", 11, 1587314572000L), Tuple3.of("k2", 2, 1587314574000L)
                , Tuple3.of("k3", 3, 1587314576000L), Tuple3.of("k2", 5, 1587314575000L)
                , Tuple3.of("k3", 10, 1587314577000L), Tuple3.of("k1", 9, 1587314579000L));
        src1.broadcast().map(new MapFunction<Tuple3<String, Integer, Long>, Object>() {
            @Override
            public Object map(Tuple3<String, Integer, Long> value) throws Exception {
                System.out.println("map当前线程："+Thread.currentThread().getName()+",数据："+value);
                return value;
            }
        }).setParallelism(2).print();
        env.execute();
        /**
         * 可以看到每条数据都发到下游的每个算子的并行度上
         * map当前线程：Map (2/2),数据：(k1,11,1587314572000)
         * map当前线程：Map (1/2),数据：(k1,11,1587314572000)
         * map当前线程：Map (1/2),数据：(k2,2,1587314574000)
         * map当前线程：Map (2/2),数据：(k2,2,1587314574000)
         * map当前线程：Map (2/2),数据：(k3,3,1587314576000)
         * map当前线程：Map (1/2),数据：(k3,3,1587314576000)
         * map当前线程：Map (2/2),数据：(k2,5,1587314575000)
         * map当前线程：Map (2/2),数据：(k3,10,1587314577000)
         * map当前线程：Map (2/2),数据：(k1,9,1587314579000)
         * map当前线程：Map (1/2),数据：(k2,5,1587314575000)
         * map当前线程：Map (1/2),数据：(k3,10,1587314577000)
         * map当前线程：Map (1/2),数据：(k1,9,1587314579000)
         */
    }
}
