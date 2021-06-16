package org.apache.flink.connector.nsq.api;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.nsq.table.descriptors.NsqValidator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.Schema.*;

public class NsqTableSourceSinkFactory
        implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Tuple2<Boolean, Row>> {

    @Override
    public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(
            Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        TableSchema tableSchema =
                TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));
        String nsqIp = descriptorProperties.getString(NsqValidator.NSQ_IP);
        int nsqPort = descriptorProperties.getInt(NsqValidator.NSQ_PORT);
        String topic = descriptorProperties.getString(NsqValidator.NSQ_TOPIC);
        int parallelism = descriptorProperties.getOptionalInt(NsqValidator.PARALLELISM).orElse(0);
        return NsqTableSink.builder()
                .setNsqIp(nsqIp)
                .setNsqPort(nsqPort)
                .setTopic(topic)
                .setParallelism(parallelism)
                .setTableSchema(tableSchema)
                .build();
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        return null;
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, NsqValidator.CONNECTOR_TYPE_VALUE_NSQ);
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        properties.add(NsqValidator.NSQ_IP);
        properties.add(NsqValidator.NSQ_PORT);
        properties.add(NsqValidator.NSQ_TOPIC);
        properties.add(NsqValidator.PARALLELISM);

        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);

        return properties;
    }

    /**
     * @param properties
     * @return
     */
    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        new NsqValidator().validate(descriptorProperties);
        return descriptorProperties;
    }
}
