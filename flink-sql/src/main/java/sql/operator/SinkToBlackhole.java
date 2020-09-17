package sql.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import sql.sink.BlackholeSink;
import sql.source.DatagenSource;

public class SinkToBlackhole {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        DatagenSource.getDatagenSource(tableEnv);
        BlackholeSink.getBlackholeSink(tableEnv);
        tableEnv.executeSql("insert into blackhole_table select f_sequence,f_random,f_random_str from datagen");
        /**executeSql异步提交任务，不需要再调用tableEnv.execute() 或 env.execute(),否则会报如下异常
         * Exception in thread "main" java.lang.IllegalStateException: No operators defined in streaming topology. Cannot generate StreamGraph.
         * 	at org.apache.flink.table.planner.utils.ExecutorUtils.generateStreamGraph(ExecutorUtils.java:47)
         * 	at org.apache.flink.table.planner.delegation.StreamExecutor.createPipeline(StreamExecutor.java:47)
         * 	at org.apache.flink.table.api.internal.TableEnvironmentImpl.execute(TableEnvironmentImpl.java:1213)
         * 	at sql.operator.SinkToBlackhole.main(SinkToBlackhole.java:20)
         *
         * 	原因是:executeSql已经是异步提交了作业，生成Transformation后会把缓存的Operation清除，见TableEnvironmentImpl#translateAndClearBuffer,
         * 	执行execute也会走那一段逻辑，报了上面异常，但是这个异常不影响程序执行
         * *
         */
        tableEnv.execute("");
    }
}
