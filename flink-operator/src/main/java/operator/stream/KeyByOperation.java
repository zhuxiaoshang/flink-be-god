package operator.stream;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * keyby算子，按某几个key做分组，相同的key会被分配到同一个subtask上
 * keySelector可以是index或fieldName
 * DataStream → KeyedStream
 */
public class KeyByOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of("k1",1),Tuple2.of("k2",2), Tuple2.of("k3",3),Tuple2.of("k2",5))
                .keyBy(0)//可以用0或t->t.f0
                .print();
        env.execute();
        /**
         * 结果
         * 2> (k1,1)
         * 6> (k2,2)
         * 1> (k3,3)
         * 6> (k2,5)
         */
    }
}
