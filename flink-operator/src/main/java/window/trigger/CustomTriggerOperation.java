package window.trigger;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import window.datasource.SourceGenerator;
import window.function.ApplyWindowFunction;

/**
 * 自定义触发器
 */
public class CustomTriggerOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src = SourceGenerator.fromElements(env);
        src.keyBy(t->t.f0).timeWindow(Time.seconds(5)).trigger(new CustomTrigger()).apply(new ApplyWindowFunction()).print();
        env.execute();
    }

    static class CustomTrigger extends Trigger<Tuple3<String, Integer, Long>, TimeWindow> {
        /**
         * Clears any state that the trigger might still hold for the given window. This is called
         * when a window is purged. Timers set using {@link TriggerContext#registerEventTimeTimer(long)}
         * and {@link TriggerContext#registerProcessingTimeTimer(long)} should be deleted here as
         * well as state acquired using {@link TriggerContext#getPartitionedState(StateDescriptor)}.
         *
         * @param window
         * @param ctx
         */
        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }

        /**
         * Called for every element that gets added to a pane. The result of this will determine
         * whether the pane is evaluated to emit results.
         *
         * @param element   The element that arrived.
         * @param timestamp The timestamp of the element that arrived.
         * @param window    The window to which the element is being added.
         * @param ctx       A context object that can be used to register timer callbacks.
         */
        @Override
        public TriggerResult onElement(Tuple3<String, Integer, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            //偶数时触发计算
            if((element.f1&1) == 1)
            {
                return TriggerResult.CONTINUE;
            }else {
                return TriggerResult.FIRE;
            }
        }

        /**
         * Called when a processing-time timer that was set using the trigger context fires.
         *
         * @param time   The timestamp at which the timer fired.
         * @param window The window for which the timer fired.
         * @param ctx    A context object that can be used to register timer callbacks.
         */
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        /**
         * Called when an event-time timer that was set using the trigger context fires.
         *
         * @param time   The timestamp at which the timer fired.
         * @param window The window for which the timer fired.
         * @param ctx    A context object that can be used to register timer callbacks.
         */
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return time == window.maxTimestamp()?TriggerResult.FIRE_AND_PURGE:TriggerResult.CONTINUE;
        }
    }
}
