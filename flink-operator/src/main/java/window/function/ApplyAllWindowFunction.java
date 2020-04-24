package window.function;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class ApplyAllWindowFunction implements AllWindowFunction<Tuple3<String, Integer, Long>, Object, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<Tuple3<String, Integer, Long>> values, Collector<Object> out) throws Exception {
        System.out.println("窗口开始时间=" + window.getStart() + ",结束时间=" + window.getEnd());
        System.out.println("窗口内数据=" + values);
        Iterator<Tuple3<String, Integer, Long>> iterator = values.iterator();
        while (iterator.hasNext()) {
            out.collect(iterator.next());
        }
    }
}
