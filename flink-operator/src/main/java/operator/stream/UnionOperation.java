package operator.stream;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * union算子，可以把两个DataStream流合成一个DataStream流
 * DataStream* → DataStream
 */
public class UnionOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, Integer, Long>> src1 = env.fromElements(Tuple3.of("k1", 11, 1587314572000L), Tuple3.of("k2", 2, 1587314574000L)
                , Tuple3.of("k3", 3, 1587314576000L), Tuple3.of("k2", 5, 1587314575000L)
                , Tuple3.of("k3", 10, 1587314577000L), Tuple3.of("k1", 9, 1587314579000L));
        DataStreamSource<Tuple3<String, Integer, Long>> src2 = env.fromElements(Tuple3.of("k4", 11, 1587314572000L),
                Tuple3.of("k5", 2, 1587314574000L));
        src1.union(src2).print();
        env.execute();
    }
}
