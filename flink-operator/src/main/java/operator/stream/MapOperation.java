package operator.stream;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * map算子的用法，一个输入对应一个输出，类型可以不相同
 * 类型不同，且输出类型是复合类型时，flink无法推断，需要显示调用.returns()指明类型，否则报异常
 * DataStream → DataStream
 */
public class MapOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements("s1","s2","s3").map(s-> Tuple2.of(s,1)).returns(new TypeHint<Tuple2<String, Integer>>() {
            @Override
            public TypeInformation<Tuple2<String, Integer>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).print();
        env.execute();
    }
}
