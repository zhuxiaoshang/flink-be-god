package operator.stream.partition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *resale算子，与rebalance类似但是不会在所有算子间轮询，数据本地传输而不是网络传输
 * 如，上游4个并行度，下游2个并行度，则上游前2个并行度对应下游第一个并行度，剩下2个对应另一个；
 *  如上游2个，下游4个，则上游第一个在下游前两个之间轮询，第二个在后两个间轮询
 *DataStream → DataStream
 */
public class RescaleOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, Long>> src1 = env.fromElements(Tuple3.of("k1", 11, 1587314572000L), Tuple3.of("k2", 2, 1587314574000L)
                , Tuple3.of("k3", 3, 1587314576000L), Tuple3.of("k2", 5, 1587314575000L)
                , Tuple3.of("k3", 10, 1587314577000L), Tuple3.of("k1", 9, 1587314579000L));
        src1.map(new MapFunction<Tuple3<String, Integer, Long>, Object>() {
            @Override
            public Object map(Tuple3<String, Integer, Long> value) throws Exception {
                System.out.println("map1当前线程："+Thread.currentThread().getName()+",数据："+value);
                return value;
            }
        }).setParallelism(2).rescale().map(new MapFunction<Object, Object>() {
            @Override
            public Object map(Object value) throws Exception {
                System.out.println("map2当前线程："+Thread.currentThread().getName()+",数据："+value);
                return value;
            }
        }).setParallelism(4).print();
        env.execute();
        /**
         * map1当前线程：Map (2/2),数据：(k2,2,1587314574000)
         * map1当前线程：Map (1/2),数据：(k1,11,1587314572000)
         * map1当前线程：Map (1/2),数据：(k3,3,1587314576000)
         * map1当前线程：Map (2/2),数据：(k2,5,1587314575000)
         * map1当前线程：Map (1/2),数据：(k3,10,1587314577000)
         * map1当前线程：Map (2/2),数据：(k1,9,1587314579000)
         * map2当前线程：Map -> Sink: Print to Std. Out (4/4),数据：(k2,5,1587314575000)
         * map2当前线程：Map -> Sink: Print to Std. Out (1/4),数据：(k1,11,1587314572000)
         * map2当前线程：Map -> Sink: Print to Std. Out (2/4),数据：(k3,3,1587314576000)
         * map2当前线程：Map -> Sink: Print to Std. Out (3/4),数据：(k2,2,1587314574000)
         * 4> (k2,5,1587314575000)
         * 2> (k3,3,1587314576000)
         * 3> (k2,2,1587314574000)
         * map2当前线程：Map -> Sink: Print to Std. Out (3/4),数据：(k1,9,1587314579000)
         * 3> (k1,9,1587314579000)
         * 1> (k1,11,1587314572000)
         * map2当前线程：Map -> Sink: Print to Std. Out (1/4),数据：(k3,10,1587314577000)
         * 1> (k3,10,1587314577000)
         */
    }
}
