package operator.stream.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import window.datasource.SourceGenerator;

/**
 * 给状态设置TTL
 * 状态清理策略：
 * cleanupInBackground(默认)：后台清理，过期状态在显式读取时删除，过期状态在下次垃圾回收时被回收
 * cleanupFullSnapshot:在做checkpoint全局snapshot时删除,不适用于RocksDB的增量checkpoint
 * cleanupIncrementally:在状态访问时，此策略将检查一组状态是否过期，并清除已过期的。如果状态后端支持，它让一个惰性迭代器以宽松的一致性遍历所有的key。
 * 这样随着时间推移，如果有任一状态被访问，所有的key都要被检查并清除。
 * 除了状态访问之外，每一个记录被执行，它也会做同样的操作。如果很多状态都用这个策略，将对每个记录进行迭代，以检查是否需要被清除。
 * 注意：增量清理将增加记录处理的延时。
 * 目前自由基于内存的backends支持这个策略。对RocksDB无效
 * 如果基于内存的backends使用的同步snapshot，由于实现的关系，它不支持并发修改，所以全局迭代器必须持有所有key的拷贝，这将增加对内存的消耗。异步snapshot则没有这个问题。
 * cleanupInRocksdbCompactFilter:在RocksDB compaction时清除过期状态。在flink每次处理了指定数量的记录后，Rocksdb压缩过滤器就会查询当前
 * 时间戳来判断状态是否过期。经常更新时间戳能够加快清理速度，但是会降低压缩性能，因为Rocksdb是用JNI调用native code来实现的。
 */
public class StateTTLOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, Integer, Long>> src = env.addSource(new SourceGenerator()).setParallelism(1);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
                /**
                 * UpdateType 更新策略
                 *OnCreateAndWrite:创建和写入时
                 *OnReadAndWrite：读和写
                 */
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                /**
                 * StateVisibility状态可见性
                 * NeverReturnExpired（默认）：过期的state不可见
                 * ReturnExpiredIfNotCleanedUp：过期的state如果还未被清除则可见
                 */
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();
        src.keyBy(0)//可以用0或t->t.f0
                .process(new KeyedProcessFunction<Tuple, Tuple3<String, Integer, Long>, Object>() {
                    ListState<Tuple3<String, Integer, Long>> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListStateDescriptor<Tuple3<String, Integer, Long>> listStateDescriptor = new ListStateDescriptor<>("list"
                                , TypeInformation.of(new TypeHint<Tuple3<String, Integer, Long>>() {
                        }));
                        listStateDescriptor.enableTimeToLive(ttlConfig);
                        listState = getRuntimeContext().getListState(listStateDescriptor);
                    }

                    /**
                     * Called when a timer set using {@link TimerService} fires.
                     *
                     * @param timestamp The timestamp of the firing timer.
                     * @param ctx       An {@link OnTimerContext} that allows querying the timestamp, the {@link TimeDomain}, and the key
                     *                  of the firing timer and getting a {@link TimerService} for registering timers and querying the time.
                     *                  The context is only valid during the invocation of this method, do not store it.
                     * @param out       The collector for returning result values.
                     * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
                     *                   to fail and may trigger recovery.
                     */
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                        System.out.println("定时器触发，timestamp="+timestamp+",current key = "+ctx.getCurrentKey()+",");
                        listState.clear();
                    }

                    @Override
                    public void processElement(Tuple3<String, Integer, Long> value, Context ctx, Collector<Object> out) throws Exception {
                        System.out.println("当前元素："+value+",timestamp="+ctx.timestamp());
                        listState.add(value);
                        ctx.timerService().registerEventTimeTimer(value.f2+5000L);
                    }
                }).print();
        env.execute();
    }

}
