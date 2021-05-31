package org.apache.flink.redis.api;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

public class RedisSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private final RedisOutputFormat redisOutputFormat;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        redisOutputFormat.setRuntimeContext(ctx);
        redisOutputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
    }

    public RedisSinkFunction(RedisOutputFormat redisOutputFormat) {
        this.redisOutputFormat = redisOutputFormat;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {}

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {}

    @Override
    public void invoke(T value, Context context) throws Exception {
        redisOutputFormat.writeRecord(value);
    }

    @Override
    public void close() throws Exception {
        redisOutputFormat.close();
        super.close();
    }
}
