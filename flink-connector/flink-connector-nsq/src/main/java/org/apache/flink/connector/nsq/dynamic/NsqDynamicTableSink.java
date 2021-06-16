package org.apache.flink.connector.nsq.dynamic;

import org.apache.flink.connector.nsq.api.NsqOutputFormat;
import org.apache.flink.connector.nsq.api.NsqSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class NsqDynamicTableSink implements DynamicTableSink {
    private String nsqIp;

    private int nsqPort;

    private String topic;

    private int parallelism;

    private TableSchema tableSchema;

    public NsqDynamicTableSink(
            String nsqIp, int nsqPort, String topic, int parallelism, TableSchema tableSchema) {
        this.nsqIp = nsqIp;
        this.nsqPort = nsqPort;
        this.topic = topic;
        this.parallelism = parallelism;
        this.tableSchema = tableSchema;
    }

    /**
     * Returns the set of changes that the sink accepts during runtime.
     *
     * <p>The planner can make suggestions but the sink has the final decision what it requires. If
     * the planner does not support this mode, it will throw an error. For example, the sink can
     * return that it only supports {@link ChangelogMode#insertOnly()}.
     *
     * @param requestedMode expected set of changes by the current plan
     */
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    /**
     * Returns a provider of runtime implementation for writing the data.
     *
     * <p>There might exist different interfaces for runtime implementation which is why {@link
     * SinkRuntimeProvider} serves as the base interface. Concrete {@link SinkRuntimeProvider}
     * interfaces might be located in other Flink modules.
     *
     * <p>Independent of the provider interface, the table runtime expects that a sink
     * implementation accepts internal data structures (see {@link RowData} for more information).
     *
     * <p>The given {@link Context} offers utilities by the planner for creating runtime
     * implementation with minimal dependencies to internal data structures.
     *
     * <p>See {@code org.apache.flink.table.connector.sink.SinkFunctionProvider} in {@code
     * flink-table-api-java-bridge}.
     *
     * @param context
     */
    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        NsqOutputFormat format =
                NsqOutputFormat.builder()
                        .setNsqIp(nsqIp)
                        .setNsqPort(nsqPort)
                        .setTopic(topic)
                        .setDataTypes(tableSchema.getFieldDataTypes())
                        .build();
        SinkFunction nsqSinkFunction = new NsqSinkFunction(format);
        return SinkFunctionProvider.of(nsqSinkFunction);
    }

    /**
     * Creates a copy of this instance during planning. The copy should be a deep copy of all
     * mutable members.
     */
    @Override
    public DynamicTableSink copy() {
        return new NsqDynamicTableSink(nsqIp, nsqPort, topic, parallelism, tableSchema);
    }

    /** Returns a string that summarizes this sink for printing to a console or log. */
    @Override
    public String asSummaryString() {
        return "nsq table sink";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String nsqIp;

        private int nsqPort;

        private String topic;

        private int parallelism;

        private TableSchema tableSchema;

        public Builder setNsqIp(String nsqIp) {
            this.nsqIp = nsqIp;
            return this;
        }

        public Builder setNsqPort(int nsqPort) {
            this.nsqPort = nsqPort;
            return this;
        }

        public Builder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder setParallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public Builder setTableSchema(TableSchema tableSchema) {
            this.tableSchema = tableSchema;
            return this;
        }

        public NsqDynamicTableSink build() {
            return new NsqDynamicTableSink(nsqIp, nsqPort, topic, parallelism, tableSchema);
        }
    }
}
