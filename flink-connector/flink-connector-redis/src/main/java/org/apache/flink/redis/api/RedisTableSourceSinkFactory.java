package org.apache.flink.redis.api;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.redis.api.util.RedisWriteStrategy;
import org.apache.flink.redis.table.descriptors.RedisValidator;
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

public class RedisTableSourceSinkFactory implements
	StreamTableSourceFactory<Row>,
	StreamTableSinkFactory<Tuple2<Boolean, Row>> {

	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));
		String redisValueType = descriptorProperties.getString(RedisValidator.REDIS_VALUE_TYPE);
		int redisPort = descriptorProperties.getInt(RedisValidator.REDIS_PORT);
		String redisIp = descriptorProperties.getString(RedisValidator.REDIS_IP);
		String redisPassword = descriptorProperties.getString(RedisValidator.REDIS_PASSWORD);
		return RedisTableSink.builder()
					.setRedisIp(redisIp)
					.setRedisPort(redisPort)
					.setRedisPassword(redisPassword)
					.setRedisValueType(redisValueType)
					.setTableSchema(tableSchema)
					.build();
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));
		String redisIp = descriptorProperties.getString(RedisValidator.REDIS_IP);
		int redisPort = descriptorProperties.getInt(RedisValidator.REDIS_PORT);
		String redisValueType = descriptorProperties.getString(RedisValidator.REDIS_VALUE_TYPE);
		boolean openCache = descriptorProperties.getOptionalBoolean(RedisValidator.OPEN_CACHE).orElse(false);
		int maxCacheSize = descriptorProperties.getOptionalInt(RedisValidator.CACHE_SIZE).orElse(10000000);
		String redisPassword = descriptorProperties.getString(RedisValidator.REDIS_PASSWORD);
		String keyAppendCharacter = descriptorProperties.getOptionalString(RedisValidator.KEY_APPEND_CHARACTER).orElse("|");
		String redisWriteStrategyString = descriptorProperties.getOptionalString(RedisValidator.STORE_KEY_STRATEGY)
			.orElse(RedisWriteStrategy.WRITE_IF_NOT_EXIST.getCode());
		RedisWriteStrategy redisWriteStrategy = RedisWriteStrategy.get(redisWriteStrategyString);
		boolean writeOpen = descriptorProperties.getOptionalBoolean(RedisValidator.WRITE_OPEN).orElse(false);

		return new RedisTableSource(tableSchema.getFieldNames(), tableSchema.getFieldDataTypes(), redisIp, redisPort,
			redisPassword, redisValueType, openCache, maxCacheSize, keyAppendCharacter, writeOpen, redisWriteStrategy);
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, RedisValidator.CONNECTOR_TYPE_VALUE_REDIS);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		properties.add(RedisValidator.REDIS_IP);
		properties.add(RedisValidator.REDIS_PORT);
		properties.add(RedisValidator.REDIS_PASSWORD);
		properties.add(RedisValidator.REDIS_VALUE_TYPE);
		properties.add(RedisValidator.OPEN_CACHE);
		properties.add(RedisValidator.CACHE_SIZE);
		properties.add(RedisValidator.KEY_APPEND_CHARACTER);
		properties.add(RedisValidator.STORE_KEY_STRATEGY);
		properties.add(RedisValidator.WRITE_OPEN);

		properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		return properties;
	}

	/**
	 *
	 * @param properties
	 * @return
	 */
	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		new RedisValidator().validate(descriptorProperties);
		return descriptorProperties;
	}
}
