package org.apache.flink.redis.api;

import org.apache.flink.redis.api.util.RedisWriteStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class RedisTableSource implements LookupableTableSource<Row>, StreamTableSource<Row> {

	private final String[] fieldNames;

	private final DataType[] fieldTypes;

	private final String ip;

	private final int port;

	private final String redisPassword;

	private final String redisValueType;

	private final boolean openCache;

	private final int cacheMaxSize;

	private String keyAppendCharacter;

	private boolean writeOpen;

	private RedisWriteStrategy redisWriteStrategy;

	public RedisTableSource(String[] fieldNames, DataType[] fieldTypes, String ip, int port, String password,
							String redisValueType, boolean openCache, int cacheMaxSize, String keyAppendCharacter,
							boolean writeOpen, RedisWriteStrategy redisWriteStrategy) {
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		this.ip = ip;
		this.port = port;
		this.redisPassword = password;
		this.redisValueType = redisValueType;
		this.openCache = openCache;
		this.cacheMaxSize = cacheMaxSize;
		this.keyAppendCharacter = keyAppendCharacter;
		this.writeOpen = writeOpen;
		this.redisWriteStrategy = redisWriteStrategy;
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		return RedisLookupFunction
			.builder()
			.setIp(ip)
			.setPort(port)
			.setRedisPassword(redisPassword)
			.setRedisValueType(redisValueType)
			.setFieldNames(fieldNames)
			.setFieldTypes(fieldTypes)
			.setRedisJoinKeys(lookupKeys)
			.setCacheMaxSize(cacheMaxSize)
			.setOpenCache(openCache)
			.setKeyAppendCharacter(keyAppendCharacter)
			.setWriteOpen(writeOpen)
			.setRedisWriteStrategy(redisWriteStrategy)
			.build();
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
		throw new UnsupportedOperationException("redis table doesn't support async lookup currently.");
	}

	@Override
	public boolean isAsyncEnabled() {
		return false;
	}

	@Override
	public TableSchema getTableSchema() {
		return TableSchema.builder()
			.fields(fieldNames, fieldTypes)
			.build();
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return null;
	}

	@Override
	public DataType getProducedDataType() {
		return getTableSchema().toRowDataType();
	}
}
