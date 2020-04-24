package operator.stream.partition;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * shuffle算子，数据随机分发到下游算子
 * 核心逻辑random.nextInt(numberOfChannels)
 * DataStream → DataStream
 */
public class ShuffleOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, Long>> src1 = env.fromElements(Tuple3.of("k1", 11, 1587314572000L), Tuple3.of("k2", 2, 1587314574000L)
                , Tuple3.of("k3", 3, 1587314576000L), Tuple3.of("k2", 5, 1587314575000L)
                , Tuple3.of("k3", 10, 1587314577000L), Tuple3.of("k1", 9, 1587314579000L));
        src1.shuffle().print();
        env.execute();
    }
}
