package operator.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flatmap算子，一个输入可以对应1一个或多个输出，类型不必相同
 * DataStream → DataStream
 */
public class FlatMapOperation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements("s1,s2,s3", "s4,s5,s6").flatMap((String s, Collector<Tuple2<String, Integer>> c) -> {
            for (String s1 : s.split(",")
            ) {
                c.collect(Tuple2.of(s1, 1));
            }
        }).returns(new TypeHint<Tuple2<String, Integer>>() {
            @Override
            public TypeInformation<Tuple2<String, Integer>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).print();
        env.execute();
    }
}
