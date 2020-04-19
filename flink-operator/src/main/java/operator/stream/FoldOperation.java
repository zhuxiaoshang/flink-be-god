package operator.stream;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * fold算子，类似于reduce，支持提供一个默认初始值，在此基础上做reduce（该算子已经废弃）
 * KeyedStream → DataStream
 */
public class FoldOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of("k1", 11), Tuple2.of("k2", 2)
                , Tuple2.of("k3", 3), Tuple2.of("k2", 5), Tuple2.of("k3", 10), Tuple2.of("k1", 9))
                .keyBy(0)//可以用0或t->t.f0
                .fold("default", new FoldFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String fold(String s, Tuple2<String, Integer> o) throws Exception {
                        return s+"-"+o.f1;
                    }
                })
                .print();
        env.execute();
        /**
         * 结果
         * 6> default-2
         * 2> default-11
         * 1> default-3
         * 1> default-3-10
         * 2> default-11-9
         * 6> default-2-5
         */
    }
}
