package operator.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * iterate算子，
 */
public class IterateOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //fromElements是非并发算子，并行度只能为1，如果调这个方法设置并行度则会报异常[2].setParallelism(8)
        DataStream<Tuple3<String, Integer, Long>> src1 = env.fromElements(Tuple3.of("k1", 11, 1587314572000L), Tuple3.of("k2", 2, 1587314574000L)
                , Tuple3.of("k3", 3, 1587314576000L), Tuple3.of("k2", 5, 1587314575000L)
                , Tuple3.of("k3", 10, 1587314577000L), Tuple3.of("k1", 9, 1587314579000L));
        IterativeStream<Tuple3<String, Integer, Long>> iterate = src1.iterate();//开启迭代流
        //循环体
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> iterateBody = iterate.map(new MapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(Tuple3<String, Integer, Long> value) throws Exception {
                return Tuple3.of(value.f0, value.f1 + 1, value.f2);
            }
        });
        //满足该条件进入循环,由于source的并行度为1，这里只能设置为1，不设置默认CPU核数，并行度必须设置和original一样，否则报异常[1]
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> feedBack = iterateBody.filter(t -> t.f1 < 5).setParallelism(1);
        iterate.closeWith(feedBack);
        //满足该条件输出
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> output = iterateBody.filter(t -> t.f1 >= 5);
        output.print();
        env.execute();
    }
//    [1]Exception in thread "main" java.lang.UnsupportedOperationException: Parallelism of the feedback stream must match the parallelism of the original stream. Parallelism of original stream: 1; parallelism of feedback stream: 8. Parallelism can be modified using DataStream#setParallelism() method
//    at org.apache.flink.streaming.api.transformations.FeedbackTransformation.addFeedbackEdge(FeedbackTransformation.java:90)
//    at org.apache.flink.streaming.api.datastream.IterativeStream.closeWith(IterativeStream.java:77)
//    at operator.stream.IterateOperation.main(IterateOperation.java:31)
//    [2]Exception in thread "main" java.lang.IllegalArgumentException: The maximum parallelism of non parallel operator must be 1.
//    at org.apache.flink.util.Preconditions.checkArgument(Preconditions.java:139)
//    at org.apache.flink.api.common.operators.util.OperatorValidationUtils.validateMaxParallelism(OperatorValidationUtils.java:59)
//    at org.apache.flink.api.common.operators.util.OperatorValidationUtils.validateMaxParallelism(OperatorValidationUtils.java:53)
//    at org.apache.flink.streaming.api.datastream.DataStreamSource.setParallelism(DataStreamSource.java:55)
//    at operator.stream.IterateOperation.main(IterateOperation.java:20)
}
