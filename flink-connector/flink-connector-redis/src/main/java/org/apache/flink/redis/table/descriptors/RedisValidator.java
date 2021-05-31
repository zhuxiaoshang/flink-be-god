package org.apache.flink.redis.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.redis.api.util.RedisWriteStrategy;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.FlinkRuntimeException;

@Internal
public class RedisValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE_REDIS = "redis";

	public static final String REDIS_IP = "redis.ip";

	public static final String REDIS_PORT = "redis.port";

	public static final String REDIS_PASSWORD = "redis.password";

	public static final String REDIS_VALUE_TYPE = "value.type";

	//写redis如果不存在
	public static final String WRITE_OPEN = "write.open";
	public static final String STORE_KEY_STRATEGY = "store.key.strategy";
	public static final String KEY_APPEND_CHARACTER = "key.append.character";

	//开启缓存
	public static final String OPEN_CACHE = "open.cache";
	public static final String CACHE_SIZE = "cache.size";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		if (properties.getOptionalBoolean(WRITE_OPEN).orElse(false)
			&& RedisWriteStrategy.ALWAYS.getCode().equals(properties.getString(STORE_KEY_STRATEGY))
			&& properties.getOptionalBoolean(OPEN_CACHE).orElse(false)) {
			throw new FlinkRuntimeException("when key.strategy is 'always',should not open cache.");
		}
	}
}
