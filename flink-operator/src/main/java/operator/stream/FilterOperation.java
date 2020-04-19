package operator.stream;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  filter算子，满足条件的返回true，不满足的为false即会被过滤掉
 *  DataStream → DataStream
 */
public class FilterOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1,3,10,9,2,7).filter(i->i>5).print();
        env.execute();
    }
}
