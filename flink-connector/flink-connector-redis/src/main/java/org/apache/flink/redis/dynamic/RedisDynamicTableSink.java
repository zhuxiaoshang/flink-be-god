package org.apache.flink.redis.dynamic;

import org.apache.flink.redis.api.RedisOutputFormat;
import org.apache.flink.redis.api.RedisSinkFunction;
import org.apache.flink.redis.api.util.DESUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/**
 * @author: zhushang
 * @create: 2020-08-27 19:03
 */
public class RedisDynamicTableSink implements DynamicTableSink {
    private String redisIp;

    private int redisPort;

    private String redisPassword;

    private String redisValueType;

    private int ttl;

    private TableSchema tableSchema;

    public RedisDynamicTableSink(
            String redisIp,
            int redisPort,
            String redisPassword,
            String redisValueType,
            TableSchema tableSchema,
            int ttl) {
        this.redisIp = redisIp;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
        this.redisValueType = redisValueType;
        this.ttl = ttl;
        this.tableSchema = tableSchema;
    }

    private RedisOutputFormat newFormat() {
        return RedisOutputFormat.builder()
                .setRedisIp(redisIp)
                .setRedisPort(redisPort)
                .setRedisPassword(DESUtils.decryptFromBase64(redisPassword))
                .setRedisValueType(redisValueType)
                .setTtl(ttl)
                .build();
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
                .addContainedKind(RowKind.DELETE)
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
        SinkFunction sinkFunction = new RedisSinkFunction(newFormat());
        return SinkFunctionProvider.of(sinkFunction);
    }

    /**
     * Creates a copy of this instance during planning. The copy should be a deep copy of all
     * mutable members.
     */
    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(
                redisIp, redisPort, redisPassword, redisValueType, tableSchema, ttl);
    }

    /** Returns a string that summarizes this sink for printing to a console or log. */
    @Override
    public String asSummaryString() {
        return "redis sink";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String redisIp;

        private int redisPort;

        private String redisPassword;

        private String redisValueType;

        private int ttl;

        private TableSchema tableSchema;

        public Builder setRedisIp(String redisIp) {
            this.redisIp = redisIp;
            return this;
        }

        public Builder setRedisPort(int redisPort) {
            this.redisPort = redisPort;
            return this;
        }

        public Builder setRedisPassword(String redisPassword) {
            this.redisPassword = redisPassword;
            return this;
        }

        public Builder setRedisValueType(String redisValueType) {
            this.redisValueType = redisValueType;
            return this;
        }

        public Builder setTableSchema(TableSchema tableSchema) {
            this.tableSchema = tableSchema;
            return this;
        }

        public Builder setTtl(int ttl) {
            this.ttl = ttl;
            return this;
        }

        public RedisDynamicTableSink build() {
            return new RedisDynamicTableSink(
                    redisIp, redisPort, redisPassword, redisValueType, tableSchema, ttl);
        }
    }
}
