package window.function;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class AggregateWindowFunction implements AggregateFunction<Tuple3<String, Integer, Long>,Tuple3<String, Integer, Long>,Integer> {
    @Override
    public Tuple3<String, Integer, Long> createAccumulator() {
        return null;
    }

    @Override
    public Tuple3<String, Integer, Long> merge(Tuple3<String, Integer, Long> o, Tuple3<String, Integer, Long> acc1) {
        return Tuple3.of(o.f0,o.f1+acc1.f1,Math.max(o.f2,acc1.f2));
    }

    @Override
    public Integer getResult(Tuple3<String, Integer, Long> o) {
        return o.f1;
    }

    @Override
    public Tuple3<String, Integer, Long> add(Tuple3<String, Integer, Long> o, Tuple3<String, Integer, Long> o2) {
        return Tuple3.of(o.f0,o.f1+o2.f1,Math.max(o.f2,o2.f2));
    }
}
