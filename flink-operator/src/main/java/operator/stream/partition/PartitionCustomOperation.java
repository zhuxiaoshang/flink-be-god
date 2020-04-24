package operator.stream.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *partitionCustom算子，自定义分区
 */
public class PartitionCustomOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Integer, Long>> src1 = env.fromElements(Tuple3.of("k1", 11, 1587314572000L), Tuple3.of("k2", 2, 1587314574000L)
                , Tuple3.of("k3", 3, 1587314576000L), Tuple3.of("k2", 5, 1587314575000L)
                , Tuple3.of("k3", 10, 1587314577000L), Tuple3.of("k1", 9, 1587314579000L));
        src1.partitionCustom(new Partitioner<Object>() {
            @Override
            public int partition(Object o, int i) {//i是总体并行度，这里是8，o是对应后面参数field对应位置的数据，这里0，所以o就是Tuple3.f0
                return o.hashCode()%i;
            }
        },0).print();
        //这个适用于元素是POJO类型，somekey指定具体字段，o为具体字段的内容
        src1.partitionCustom(new Partitioner<Object>() {
            @Override
            public int partition(Object o, int i) {
                return 0;
            }
        },"someKey");
        //第二个参数传递一个keySelector,类似于第一种
        src1.partitionCustom(new Partitioner<Object>() {
            @Override
            public int partition(Object o, int i) {
                return 0;
            }
        }, new KeySelector<Tuple3<String, Integer, Long>, Object>() {
            @Override
            public Object getKey(Tuple3<String, Integer, Long> tuple3) throws Exception {
                return tuple3.f1;
            }
        });

        env.execute();
    }
}
