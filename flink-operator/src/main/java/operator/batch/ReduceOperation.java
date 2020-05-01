package operator.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class ReduceOperation {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple3.of(1.0, 2.0, 1), Tuple3.of(2.0, 2.0, 1),Tuple3.of(3.0,2.0,1), Tuple3.of(1.0, 2.0, 2), Tuple3.of(2.0, 2.0, 2),Tuple3.of(3.0,2.0,2))
                .map((MapFunction<Tuple3<Double, Double, Integer>, Tuple3<String, Double, Integer>>) t -> Tuple3.of(String.valueOf(t.f0), t.f1, t.f2)).groupBy(2)
                .reduce((ReduceFunction<Tuple3<String, Double, Integer>>) (tuple, t1) -> Tuple3.of(tuple.f0 + "#" + t1.f0, tuple.f1, tuple.f2)).print();
        env.execute();
    }
}
