package window.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * processFunction,继承自AbstractRichFunction，相比于applyFunction可以获取上下文中的状态
 * getRuntimeContext().getState()
 */
public class ProcessingWindowFunction extends ProcessWindowFunction<Tuple3<String, Integer, Long>, Object, String,
        TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<Tuple3<String, Integer, Long>> elements, Collector<Object> out) throws Exception {
        context.globalState();
        context.windowState();
        //do something
        System.out.println("窗口开始时间=" + context.window().getStart() + ",结束时间=" + context.window().getEnd());
        System.out.println("key = " + key);
        System.out.println("窗口内数据=" + elements);
    }
}
