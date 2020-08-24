package jira;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class Accumulators {
    public static final Logger LOG = LoggerFactory.getLogger(Accumulators.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (int i = 0; i < 1000; i++) {
                    ctx.collect("t==="+i);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(1).flatMap((String s, Collector<Tuple2<String, Integer>> c) -> {
            for (String s1 : s.split(",")
            ) {
                c.collect(Tuple2.of(s1, 1));
            }
        }).returns(new TypeHint<Tuple2<String, Integer>>() {
            @Override
            public TypeInformation<Tuple2<String, Integer>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).setParallelism(1).print().setParallelism(1);

        JobClient client = env.executeAsync("test");
        CompletableFuture<JobStatus> status = client.getJobStatus();
        LOG.info("status = " + status.get());
        System.out.println("status = " + status.get());
        CompletableFuture<JobExecutionResult> jobExecutionResult = client.getJobExecutionResult(Accumulators.class.getClassLoader());

        System.out.println(jobExecutionResult.get(5,TimeUnit.SECONDS));

        CompletableFuture<Map<String, Object>> accumulators = client.getAccumulators(Accumulators.class.getClassLoader());
       LOG.info("accus = " + accumulators.get(5, TimeUnit.SECONDS));
        System.out.println("accus = " + accumulators.get(5, TimeUnit.SECONDS));
    }
}
