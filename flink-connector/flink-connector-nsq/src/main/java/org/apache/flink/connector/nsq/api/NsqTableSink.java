package org.apache.flink.connector.nsq.api;

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

public class NsqTableSink implements UpsertStreamTableSink<Row> {

    private String nsqIp;

    private int nsqPort;

    private String topic;

    private int parallelism;

    private TableSchema tableSchema;

    public NsqTableSink(
            String nsqIp, int nsqPort, String topic, int parallelism, TableSchema tableSchema) {
        this.nsqIp = nsqIp;
        this.nsqPort = nsqPort;
        this.topic = topic;
        this.parallelism = parallelism;
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
        int sinkerParallelism = parallelism == 0 ? dataStream.getParallelism() : parallelism;
        return dataStream
                .addSink(new NsqSinkFunction(newFormat()))
                .setParallelism(sinkerParallelism)
                .name(
                        TableConnectorUtils.generateRuntimeName(
                                this.getClass(), tableSchema.getFieldNames()));
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(
            String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return new NsqTableSink(nsqIp, nsqPort, topic, parallelism, tableSchema);
    }

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }

    private NsqOutputFormat newFormat() {
        return NsqOutputFormat.builder()
                .setNsqIp(nsqIp)
                .setNsqPort(nsqPort)
                .setTopic(topic)
                .setDataTypes(tableSchema.getFieldDataTypes())
                .build();
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

        public NsqTableSink build() {
            return new NsqTableSink(nsqIp, nsqPort, topic, parallelism, tableSchema);
        }
    }
}
