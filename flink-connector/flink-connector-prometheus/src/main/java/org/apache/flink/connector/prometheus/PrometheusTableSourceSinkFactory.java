package org.apache.flink.connector.prometheus;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.prometheus.PrometheusConnectorDescriptorValidator.*;
import static org.apache.flink.table.descriptors.Schema.*;

public class PrometheusTableSourceSinkFactory implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Row> {
	@Override
	public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		//validate properties
		new PrometheusConnectorDescriptorValidator().validate(descriptorProperties);
		final TableSchema schema = TableSchemaUtils.getPhysicalSchema(
			descriptorProperties.getTableSchema(SCHEMA));
		String address = descriptorProperties.getString(CONNECTOR_ADDRESS);
		int parallelism = descriptorProperties.getOptionalInt(PARALLELISM).orElse(-1);
		return new PrometheusTableSink(address, schema,parallelism);
	}


	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> map) {
		return null;
	}

	/**
	 * 必填参数
	 * @return
	 */
	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>(1);
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_PROMETHEUS);
		return context;
	}

	/**
	 * 支持的参数
	 * @return
	 */
	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(CONNECTOR_ADDRESS);
		properties.add(PARALLELISM);
		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		return properties;
	}
}
