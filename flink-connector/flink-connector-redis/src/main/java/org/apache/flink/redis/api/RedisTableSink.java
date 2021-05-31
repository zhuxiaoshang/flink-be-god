package org.apache.flink.redis.api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

public class RedisTableSink implements UpsertStreamTableSink<Row> {

    private String redisIp;

    private int redisPort;

    private String redisPassword;

    private String redisValueType;

    private TableSchema tableSchema;

    public RedisTableSink(
            String redisIp,
            int redisPort,
            String redisPassword,
            String redisValueType,
            TableSchema tableSchema) {
        this.redisIp = redisIp;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
        this.redisValueType = redisValueType;
        this.tableSchema = tableSchema;
    }

    @Override
    public void setKeyFields(String[] keys) {}

    @Override
    public void setIsAppendOnly(Boolean isAppendOnly) {}

    @Override
    public TypeInformation<Row> getRecordType() {
        return (TypeInformation<Row>)
                TypeConversions.fromDataTypeToLegacyInfo(tableSchema.toRowDataType());
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return dataStream
                .addSink(new RedisSinkFunction(newFormat()))
                .setParallelism(dataStream.getParallelism())
                .name(
                        TableConnectorUtils.generateRuntimeName(
                                this.getClass(), tableSchema.getFieldNames()));
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(
            String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return new RedisTableSink(redisIp, redisPort, redisPassword, redisValueType, tableSchema);
    }

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }

    private RedisOutputFormat newFormat() {
        return RedisOutputFormat.builder()
                .setRedisIp(redisIp)
                .setRedisPort(redisPort)
                .setRedisPassword(redisPassword)
                .setRedisValueType(redisValueType)
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String redisIp;

        private int redisPort;

        private String redisPassword;

        private String redisValueType;

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

        public RedisTableSink build() {
            return new RedisTableSink(
                    redisIp, redisPort, redisPassword, redisValueType, tableSchema);
        }
    }
}
