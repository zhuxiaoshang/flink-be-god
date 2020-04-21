package operator.stream.partition;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * rebalance算子，数据轮询分发给下游算子
 * 核心逻辑nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels,用一个计数器记录下一个要发送的channel
 * DataStream → DataStream
 */
public class RebalanceOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, Long>> src1 = env.fromElements(Tuple3.of("k1", 11, 1587314572000L), Tuple3.of("k2", 2, 1587314574000L)
                , Tuple3.of("k3", 3, 1587314576000L), Tuple3.of("k2", 5, 1587314575000L)
                , Tuple3.of("k3", 10, 1587314577000L), Tuple3.of("k1", 9, 1587314579000L));
        src1.broadcast().print();
        env.execute();
    }
}
