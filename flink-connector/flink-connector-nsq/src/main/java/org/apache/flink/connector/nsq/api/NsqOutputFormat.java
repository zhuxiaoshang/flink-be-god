package org.apache.flink.connector.nsq.api;

import com.github.brainlag.nsq.NSQProducer;
import com.github.brainlag.nsq.exceptions.NSQException;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class NsqOutputFormat<T> extends RichOutputFormat<T> {

    private static final Logger LOG = LoggerFactory.getLogger(NsqOutputFormat.class);

    private String nsqIp;

    private int nsqPort;

    private String topic;

    private transient NSQProducer nsqProducer;

    private transient ObjectMapper objectMapper;

    private transient RateLimiter rateLimiter;

    private DataType[] dataTypes;

    public NsqOutputFormat(String nsqIp, int nsqPort, String topic, DataType[] dataTypes) {
        this.nsqIp = nsqIp;
        this.nsqPort = nsqPort;
        this.topic = topic;
        this.dataTypes = dataTypes;
    }

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

        objectMapper = new ObjectMapper();

        rateLimiter = RateLimiter.create(5.0d);

        nsqProducer = new NSQProducer();
        nsqProducer.addAddress(nsqIp, nsqPort);
        nsqProducer.start();
    }

    @Override
    public void writeRecord(T value) throws IOException {
        Row row;
        RowData rowData;
        byte[] msg;
        if (value instanceof Tuple2) {
            row = ((Tuple2<Boolean, Row>) value).f1;
            msg = objectMapper.writeValueAsBytes(row.getField(0));
        } else if (value instanceof RowData) {
            rowData = (RowData) value;
            BinaryMapData binaryMapData =
                    (BinaryMapData)
                            RowData.createFieldGetter(dataTypes[0].getLogicalType(), 0)
                                    .getFieldOrNull(rowData);
            msg =
                    objectMapper.writeValueAsBytes(
                            getMapValue(binaryMapData, dataTypes[0].getLogicalType()));
        } else {
            LOG.error("record type is unsupported.record is {}", value);
            throw new FlinkRuntimeException(
                    String.format("record type is unsupported.record is %s", value));
        }
        boolean failFast = false;
        try {
            nsqProducer.produce(topic, msg);
        } catch (NSQException e) {
            failFast = !rateLimiter.tryAcquire();
            LOG.error("", e);
        } catch (TimeoutException e) {
            failFast = !rateLimiter.tryAcquire();
            LOG.error("", e);
        }

        if (failFast) {
            throw new RuntimeException(
                    "Send nsq failed frequently, five time per second, do fail fast.");
        }
    }

    private Map<String, String> getMapValue(BinaryMapData binaryMapData, LogicalType logicalType) {
        Map<?, ?> tmpMap =
                binaryMapData.toJavaMap(
                        logicalType.getChildren().get(0), logicalType.getChildren().get(1));
        Map<String, String> res = new HashMap<>();
        for (Map.Entry entry : tmpMap.entrySet()) {
            res.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return res;
    }

    @Override
    public void close() throws IOException {
        nsqProducer.shutdown();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** RedisOutputFormat 的Builder */
    public static class Builder {

        private String nsqIp;

        private int nsqPort;

        private String topic;

        private DataType[] dataTypes;

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

        public Builder setDataTypes(DataType[] dataTypes) {
            this.dataTypes = dataTypes;
            return this;
        }

        public NsqOutputFormat build() {
            return new NsqOutputFormat(nsqIp, nsqPort, topic, dataTypes);
        }
    }
}
