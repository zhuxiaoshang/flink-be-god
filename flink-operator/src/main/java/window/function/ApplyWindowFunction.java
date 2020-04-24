package window.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ApplyWindowFunction implements WindowFunction<Tuple3<String, Integer, Long>, Object, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<String, Integer, Long>> input, Collector<Object> out) throws Exception {
        //do something
        System.out.println("窗口开始时间="+window.getStart()+",结束时间="+window.getEnd());
        System.out.println("key = "+tuple);
        System.out.println("窗口内数据="+input);
    }
}
