package connector.end2endexactlyonce;

import connector.kafka.KafkaConnector;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class MysqlSinkExactlyOnceMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //checkpoint interval 10s
        env.enableCheckpointing(4000L);
        //设置模式为：exactly_one，仅一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //同一时间只允许进行一次检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setStateBackend(new FsStateBackend("file:///Users/zhushang/Desktop/software/backend"));

        DataStream<String> kafkaSource = KafkaConnector.getKafkaSource(env);
        SingleOutputStreamOperator<List<String>> apply =
                kafkaSource.timeWindowAll(Time.seconds(2)).apply(new AllWindowFunction<String, List<String>,
                        TimeWindow>() {
            /**
             * Evaluates the window and outputs none or several elements.
             *
             * @param window The window that is being evaluated.
             * @param values The elements in the window being evaluated.
             * @param out    A collector for emitting elements.
             * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
             */
            @Override
            public void apply(TimeWindow window, Iterable<String> values, Collector<List<String>> out) throws Exception {
                List<String> list = new ArrayList<>();
                for (String v :
                        values) {
                    list.add(v);
                }
                out.collect(list);
            }
        });
        apply.addSink(new MysqlSink());
        /**
         [1]直接使用kafka的数据源会报如下错误：
         java.lang.Exception: Could not materialize checkpoint 1 for operator Source: Custom Source -> Sink: Unnamed (1/1).
         at org.apache.flink.streaming.runtime.tasks.StreamTask$AsyncCheckpointRunnable.handleExecutionException(StreamTask.java:1221)
         at org.apache.flink.streaming.runtime.tasks.StreamTask$AsyncCheckpointRunnable.run(StreamTask.java:1163)
         at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
         at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
         at java.lang.Thread.run(Thread.java:748)
         Caused by: java.util.concurrent.ExecutionException: java.io.IOException: Size of the state is larger than the maximum permitted memory-backed state. Size=32967000 , maxSize=5242880 . Consider using a different state backend, like the File System State backend.
         at java.util.concurrent.FutureTask.report(FutureTask.java:122)
         at java.util.concurrent.FutureTask.get(FutureTask.java:192)
         at org.apache.flink.runtime.concurrent.FutureUtils.runIfNotDoneAndGet(FutureUtils.java:461)
         at org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer.<init>(OperatorSnapshotFinalizer.java:53)
         at org.apache.flink.streaming.runtime.tasks.StreamTask$AsyncCheckpointRunnable.run(StreamTask.java:1126)
         ... 3 more
         Caused by: java.io.IOException: Size of the state is larger than the maximum permitted memory-backed state. Size=32967000 , maxSize=5242880 . Consider using a different state backend, like the File System State backend.
         at org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory.checkSize(MemCheckpointStreamFactory.java:64)
         at org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory$MemoryCheckpointOutputStream.closeAndGetBytes(MemCheckpointStreamFactory.java:145)
         at org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory$MemoryCheckpointOutputStream.closeAndGetHandle(MemCheckpointStreamFactory.java:126)
         at org.apache.flink.runtime.state.DefaultOperatorStateBackendSnapshotStrategy$1.callInternal(DefaultOperatorStateBackendSnapshotStrategy.java:179)
         at org.apache.flink.runtime.state.DefaultOperatorStateBackendSnapshotStrategy$1.callInternal(DefaultOperatorStateBackendSnapshotStrategy.java:108)
         at org.apache.flink.runtime.state.AsyncSnapshotCallable.call(AsyncSnapshotCallable.java:75)
         at java.util.concurrent.FutureTask.run(FutureTask.java:266)
         at org.apache.flink.runtime.concurrent.FutureUtils.runIfNotDoneAndGet(FutureUtils.java:458)
         ... 5 more

         原因是本地测试没有设置状态后端，默认使用的是memorybackend，状态大小超过最大限制。

         [2]设置了fsbackend后上面的问题[1]没有了,又报了下面的错误：com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException: Communications link failure during commit(). Transaction resolution unknown.
         [Source: Custom Source -> Sink: Unnamed (1/1)] ERROR connector.end2endexactlyonce.DBConnectUtil - 提交事物失败,Connection:com.mysql.jdbc.JDBC4Connection@7d7d3790
         com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException: Communications link failure during commit(). Transaction resolution unknown.
         at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
         at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
         at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
         at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
         at com.mysql.jdbc.Util.handleNewInstance(Util.java:425)
         at com.mysql.jdbc.Util.getInstance(Util.java:408)
         at com.mysql.jdbc.SQLError.createSQLException(SQLError.java:919)
         at com.mysql.jdbc.SQLError.createSQLException(SQLError.java:898)
         at com.mysql.jdbc.SQLError.createSQLException(SQLError.java:887)
         at com.mysql.jdbc.SQLError.createSQLException(SQLError.java:861)
         at com.mysql.jdbc.ConnectionImpl.commit(ConnectionImpl.java:1559)
         at connector.end2endexactlyonce.DBConnectUtil.commit(DBConnectUtil.java:52)
         at connector.end2endexactlyonce.MysqlSink.commit(MysqlSink.java:96)
         at connector.end2endexactlyonce.MysqlSink.commit(MysqlSink.java:16)
         at org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction.notifyCheckpointComplete(TwoPhaseCommitSinkFunction.java:289)
         at org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator.notifyCheckpointComplete(AbstractUdfStreamOperator.java:130)
         at org.apache.flink.streaming.runtime.tasks.StreamTask.lambda$notifyCheckpointComplete$8(StreamTask.java:919)
         at org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor$SynchronizedStreamTaskActionExecutor.call(StreamTaskActionExecutor.java:101)
         at org.apache.flink.streaming.runtime.tasks.StreamTask.notifyCheckpointComplete(StreamTask.java:913)
         at org.apache.flink.streaming.runtime.tasks.StreamTask.lambda$notifyCheckpointCompleteAsync$7(StreamTask.java:907)
         at org.apache.flink.util.function.FunctionUtils.lambda$asCallable$5(FunctionUtils.java:125)
         at java.util.concurrent.FutureTask.run(FutureTask.java:266)
         at org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor$SynchronizedStreamTaskActionExecutor.run(StreamTaskActionExecutor.java:87)
         at org.apache.flink.streaming.runtime.tasks.mailbox.Mail.run(Mail.java:78)
         at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.processMail(MailboxProcessor.java:261)
         at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:186)
         at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:485)
         at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:469)
         at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:708)
         at org.apache.flink.runtime.taskmanager.Task.run(Task.java:533)
         at java.lang.Thread.run(Thread.java:748)
         [Source: Custom Source -> Sink: Unnamed (1/1)] ERROR connector.end2endexactlyonce.DBConnectUtil - 关闭连接失败,Connection:com.mysql.jdbc.JDBC4Connection@7d7d3790
         com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException: Communications link failure during rollback(). Transaction resolution unknown.
         at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
         at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
         at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
         at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
         at com.mysql.jdbc.Util.handleNewInstance(Util.java:425)
         at com.mysql.jdbc.Util.getInstance(Util.java:408)
         at com.mysql.jdbc.SQLError.createSQLException(SQLError.java:919)
         at com.mysql.jdbc.SQLError.createSQLException(SQLError.java:898)
         at com.mysql.jdbc.SQLError.createSQLException(SQLError.java:887)
         at com.mysql.jdbc.SQLError.createSQLException(SQLError.java:861)
         at com.mysql.jdbc.ConnectionImpl.rollback(ConnectionImpl.java:4572)
         at com.mysql.jdbc.ConnectionImpl.realClose(ConnectionImpl.java:4198)
         at com.mysql.jdbc.ConnectionImpl.close(ConnectionImpl.java:1472)
         at connector.end2endexactlyonce.DBConnectUtil.close(DBConnectUtil.java:88)
         at connector.end2endexactlyonce.DBConnectUtil.commit(DBConnectUtil.java:57)
         at connector.end2endexactlyonce.MysqlSink.commit(MysqlSink.java:96)
         at connector.end2endexactlyonce.MysqlSink.commit(MysqlSink.java:16)
         at org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction.notifyCheckpointComplete(TwoPhaseCommitSinkFunction.java:289)
         at org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator.notifyCheckpointComplete(AbstractUdfStreamOperator.java:130)
         at org.apache.flink.streaming.runtime.tasks.StreamTask.lambda$notifyCheckpointComplete$8(StreamTask.java:919)
         at org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor$SynchronizedStreamTaskActionExecutor.call(StreamTaskActionExecutor.java:101)
         at org.apache.flink.streaming.runtime.tasks.StreamTask.notifyCheckpointComplete(StreamTask.java:913)
         at org.apache.flink.streaming.runtime.tasks.StreamTask.lambda$notifyCheckpointCompleteAsync$7(StreamTask.java:907)
         at org.apache.flink.util.function.FunctionUtils.lambda$asCallable$5(FunctionUtils.java:125)
         at java.util.concurrent.FutureTask.run(FutureTask.java:266)
         at org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor$SynchronizedStreamTaskActionExecutor.run(StreamTaskActionExecutor.java:87)
         at org.apache.flink.streaming.runtime.tasks.mailbox.Mail.run(Mail.java:78)
         at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.processMail(MailboxProcessor.java:261)
         at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:186)
         at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:485)
         at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:469)
         at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:708)
         at org.apache.flink.runtime.taskmanager.Task.run(Task.java:533)
         at java.lang.Thread.run(Thread.java:748)
         最开始设置的是10s一次checkpoint，10s才会往mysql提交一次数据，由于kafka数据量比较大，可能会导致大批量的插入导致心跳超时
         参考：https://community.talend.com/t5/Design-and-Development/Communications-link-failure-during-commit-MySQL/td-p/46417
         当我们把checkpoint间隔改成1s一次时，已经有部分数据能提交成功了
         尝试改成批量插入
         */



        DataStream<String> src = env.addSource(new RichSourceFunction<String>() {
            String[] element ={"{\"user_id\": \"508131\", \"item_id\":\"2279223\", \"category_id\": \"3299155\", " +
                    "\"behavior\": \"pv\", \"ts\": \"2017-11-27T00:14:19Z\"}","{\"user_id\": \"417559\", " +
                    "\"item_id\":\"325080\", \"category_id\": \"1829221\", \"behavior\": \"pv\", \"ts\": " +
                    "\"2017-11-27T00:14:19Z\"}","{\"user_id\": \"376626\", \"item_id\":\"146455\", \"category_id\": " +
                    "\"1851156\", \"behavior\": \"pv\", \"ts\": \"2017-11-27T00:14:19Z\"}","{\"user_id\": \"53744\", " +
                    "\"item_id\":\"2528364\", \"category_id\": \"2520377\", \"behavior\": \"pv\", \"ts\": " +
                    "\"2017-11-27T00:14:19Z\"}","{\"user_id\": \"274574\", \"item_id\":\"1847862\", \"category_id\": " +
                    "\"3002561\", \"behavior\": \"pv\", \"ts\": \"2017-11-27T00:14:19Z\"}","{\"user_id\": \"244926\"," +
                    " \"item_id\":\"619901\", \"category_id\": \"2735466\", \"behavior\": \"pv\", \"ts\": " +
                    "\"2017-11-27T00:14:19Z\"}","{\"user_id\": \"390985\", \"item_id\":\"3017074\", \"category_id\": " +
                    "\"2920476\", \"behavior\": \"pv\", \"ts\": \"2017-11-27T00:14:19Z\"}","{\"user_id\": \"338676\"," +
                    " \"item_id\":\"3674363\", \"category_id\": \"2578647\", \"behavior\": \"pv\", \"ts\": " +
                    "\"2017-11-27T00:14:19Z\"}","{\"user_id\": \"252190\", \"item_id\":\"4689745\", \"category_id\": " +
                    "\"672001\", \"behavior\": \"pv\", \"ts\": \"2017-11-27T00:14:19Z\"}","{\"user_id\": \"409804\", " +
                    "\"item_id\":\"3288183\", \"category_id\": \"1573426\", \"behavior\": \"pv\", \"ts\": " +
                    "\"2017-11-27T00:14:19Z\"}","{\"user_id\": \"453109\", \"item_id\":\"2537924\", \"category_id\": " +
                    "\"3607361\", \"behavior\": \"pv\", \"ts\": \"2017-11-27T00:14:19Z\"}"};

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int idx = 0;
                while(true){
                    ctx.collect(element[idx]);
                    idx = (idx+1)%element.length;
                    Thread.sleep(10L);
                }

            }

            @Override
            public void cancel() {

            }
        });
//        src.addSink(new MysqlSink());
        env.execute();
        /**
         * 结果如下
         *
         * select count(*) from mysql_test;
         * +----------+
         * | count(*) |
         * +----------+
         * |       35 |
         * +----------+
         * 1 row in set (0.00 sec)
         *
         *
         * select * from mysql_test;
         * +---------+---------+-------------+----------+---------------------+
         * | user_id | item_id | category_id | behavior | ts                  |
         * +---------+---------+-------------+----------+---------------------+
         * |  508131 | 2279223 |     3299155 | pv       | 2017-11-27 08:14:19 |
         * |  417559 |  325080 |     1829221 | pv       | 2017-11-27 08:14:19 |
         * |  376626 |  146455 |     1851156 | pv       | 2017-11-27 08:14:19 |
         * |   53744 | 2528364 |     2520377 | pv       | 2017-11-27 08:14:19 |
         * |  274574 | 1847862 |     3002561 | pv       | 2017-11-27 08:14:19 |
         * |  244926 |  619901 |     2735466 | pv       | 2017-11-27 08:14:19 |
         * |  390985 | 3017074 |     2920476 | pv       | 2017-11-27 08:14:19 |
         * |  338676 | 3674363 |     2578647 | pv       | 2017-11-27 08:14:19 |
         * |  252190 | 4689745 |      672001 | pv       | 2017-11-27 08:14:19 |
         * |  409804 | 3288183 |     1573426 | pv       | 2017-11-27 08:14:19 |
         * |  453109 | 2537924 |     3607361 | pv       | 2017-11-27 08:14:19 |
         * |  508131 | 2279223 |     3299155 | pv       | 2017-11-27 08:14:19 |
         * |  417559 |  325080 |     1829221 | pv       | 2017-11-27 08:14:19 |
         * |  376626 |  146455 |     1851156 | pv       | 2017-11-27 08:14:19 |
         * |   53744 | 2528364 |     2520377 | pv       | 2017-11-27 08:14:19 |
         * |  274574 | 1847862 |     3002561 | pv       | 2017-11-27 08:14:19 |
         * |  244926 |  619901 |     2735466 | pv       | 2017-11-27 08:14:19 |
         * |  390985 | 3017074 |     2920476 | pv       | 2017-11-27 08:14:19 |
         * |  338676 | 3674363 |     2578647 | pv       | 2017-11-27 08:14:19 |
         * |  252190 | 4689745 |      672001 | pv       | 2017-11-27 08:14:19 |
         * |  409804 | 3288183 |     1573426 | pv       | 2017-11-27 08:14:19 |
         * |  453109 | 2537924 |     3607361 | pv       | 2017-11-27 08:14:19 |
         * |  508131 | 2279223 |     3299155 | pv       | 2017-11-27 08:14:19 |
         */
    }
}
