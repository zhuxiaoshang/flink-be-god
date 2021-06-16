package org.apache.flink.connector.nsq.api;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class NsqSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private final NsqOutputFormat nsqOutputFormat;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        nsqOutputFormat.setRuntimeContext(ctx);
        nsqOutputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
    }

    public NsqSinkFunction(NsqOutputFormat nsqOutputFormat) {
        this.nsqOutputFormat = nsqOutputFormat;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {}

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {}

    @Override
    public void invoke(T value, Context context) throws Exception {
        nsqOutputFormat.writeRecord(value);
    }

    @Override
    public void close() throws Exception {
        nsqOutputFormat.close();
        super.close();
    }
}
