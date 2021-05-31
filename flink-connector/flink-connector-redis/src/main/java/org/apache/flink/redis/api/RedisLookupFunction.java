package org.apache.flink.redis.api;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.redis.api.util.RedisConnectionUtils;
import org.apache.flink.redis.api.util.RedisValueType;
import org.apache.flink.redis.api.util.RedisWriteStrategy;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RedisLookupFunction extends TableFunction<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(RedisLookupFunction.class);

	private static final long serialVersionUID = 1L;
	
	//缓存的最大大小
	private static final int DEFAULT_MAX_SIZE = 10000000;

	/**
	 * Redis的IP
	 */
	private String ip;

	/**
	 * Redis的Port
	 */
	private int port;

	/**
	 *
	 */
	private String redisPassword;

	/**
	 * KEY的连接字符串
	 */
	private String keyAppendCharacter;

	/**
	 * 是否开启写入redis
	 */
	private boolean writeOpen;

	/**
	 * 写入redis的策略
	 */
	private RedisWriteStrategy redisWriteStrategy;

	/**
	 * Redis的KEY的存储值的类型
	 */
	private String redisValueType;

	/**
	 * KEY的存储的value的类型(内部类型)
	 */
	private RedisValueType type;


	private boolean openCache;
	private int cacheMaxSize;

	/**
	 * Row的字段名
	 */
	private String[] fieldNames;

	/**
	 * Row的字段类型
	 */
	private DataType[] fieldTypes;

	private transient JedisPoolConfig config;

	private transient JedisPool jedisPool;

	private transient LRUMap cache;


	public RedisLookupFunction(String ip, int port, String password, String[] fieldNames, DataType[] fieldTypes,
							   String[] redisJoinKeys, String redisValueType, boolean openCache, int cacheMaxSize,
							   String keyAppendCharacter, boolean writeOpen, RedisWriteStrategy redisWriteStrategy) {
		this.ip = ip;
		this.port = port;
		this.redisPassword = password;
		this.redisValueType = redisValueType;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		this.openCache = openCache;
		this.cacheMaxSize = cacheMaxSize;
		this.keyAppendCharacter = keyAppendCharacter;
		this.writeOpen = writeOpen;
		this.redisWriteStrategy = redisWriteStrategy;
		this.type = RedisValueType.getRedisValueType(this.redisValueType);
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		config = RedisConnectionUtils.getJedisPoolConfig();
		jedisPool = RedisConnectionUtils.getJedisPool(config, ip, port, redisPassword);
		if(openCache) {
			if(cacheMaxSize <= 0 || cacheMaxSize > DEFAULT_MAX_SIZE ) {
				cacheMaxSize = DEFAULT_MAX_SIZE;
			}
			cache = new LRUMap(this.cacheMaxSize);
		}
	}

	@Override
	public TypeInformation<Row> getResultType() {
		DataType dataType = TableSchema.builder().fields(fieldNames, fieldTypes).build().toRowDataType();
		return (TypeInformation<Row>)TypeConversions.fromDataTypeToLegacyInfo(dataType);
	}

	@Override
	public void close() throws Exception {
		RedisConnectionUtils.closeJedisPool(jedisPool);
	}

	//计算
	public void eval(Object... params) {
		Pair<Boolean, Object> pair = null;
		try {
			String joinKeyVal = String.valueOf(params[0]);
			String redisKey = joinKeyVal;
			String[] values = null;

			//查询redis的同时写redis
			if(writeOpen) {
				values = StringUtils.split(joinKeyVal, keyAppendCharacter);
				redisKey = values[values.length-1];
			}

			pair = getObjectFromRedis(redisKey);
			//redis不存在则写
			if(writeOpen &&
				(redisWriteStrategy == RedisWriteStrategy.ALWAYS ||
					(redisWriteStrategy == RedisWriteStrategy.WRITE_IF_NOT_EXIST && !pair.getLeft()))) {
				setKVToRedis(values);
			}

			collect(Row.of(redisKey, pair.getRight()));
		} catch (Exception e) {
			LOG.error(String.format("redis lookup join failed, params [%s], pair [%s]", Arrays.toString(params),
				(null == pair ? "null" : pair.getLeft() + ":" + pair.getRight())), e);
		}
	}

	/**
	 * 写redis
	 * @param keyValues
	 */
	public void setKVToRedis(String[] keyValues) {
		switch (type) {
			case REDIS_VALUE_TYPE_STRING:
				setStringToRedis(keyValues);
				break;
			case REDIS_VALUE_TYPE_HASH:
				setHashToRedis(keyValues);
				break;
			default:
				throw new RuntimeException(String.format("unsupport redis value type on join [%s], support (string, hash).", type));
		}
	}

	public void setStringToRedis(String[] keyValues) {
		String key = keyValues[keyValues.length-1];
		String value = keyValues[keyValues.length-2];
		try(Jedis jedis = jedisPool.getResource()) {
			jedis.set(key, value);
		}
	}

	public void setHashToRedis(String[] keyValues) {
		String key = keyValues[keyValues.length-1];
		Map<String, String> hash = new HashMap<>();
		for(int i=0, j=1; j<keyValues.length-1; i=i+2, j=j+2) {
			hash.put(keyValues[i], keyValues[j]);
		}

		try(Jedis jedis = jedisPool.getResource()) {
			jedis.hmset(key, hash);
		}
	}

	/**
	 *
	 * @param redisKey
	 * @return
	 */
	public Pair<Boolean, Object> getObjectFromRedis(String redisKey) {
		Object value = null;
		if(openCache) {
			value = cache.get(redisKey);
		}

		Pair<Boolean, Object> pair; //<redis中是否存在该Key, key对应的value>
		if(null == value) {
			switch (type) {
				case REDIS_VALUE_TYPE_STRING:
					pair = getStringValue(redisKey);
					break;
				case REDIS_VALUE_TYPE_HASH:
					pair = getHashValue(redisKey);
					break;
				case REDIS_VALUE_TYPE_LIST:
					pair = getListValue(redisKey);
					break;
				case REDIS_VALUE_TYPE_SET:
					pair = getSetValue(redisKey);
					break;
				case REDIS_VALUE_TYPE_ZSET:
					pair = getZSetValue(redisKey);
					break;
				default:
					//can not reach!!!
					throw new RuntimeException(String.format("unknow redis value type [%s]", type));
			}

			if(openCache && pair.getLeft()) {
				cache.put(redisKey, pair.getRight());
			}

			return pair;
		} else {
			return Pair.of(true, value);
		}
	}

	private Pair<Boolean, Object> getStringValue(String redisKey) {
		try(Jedis jedis = jedisPool.getResource()) {
			String value = jedis.get(redisKey);
			return Pair.of(null == value ? false : true, value);
		} catch (Exception e) {
			LOG.error("query redis failed.", e);
			return Pair.of(false, null);
		}
	}

	private Pair<Boolean, Object> getHashValue(String redisKey) {
		try(Jedis jedis = jedisPool.getResource()) {
			Map<String, String> hash = jedis.hgetAll(redisKey);
			return Pair.of(hash.size() == 0 ? false : true, hash);
		} catch (Exception e) {
			LOG.error("query redis failed.", e);
			return Pair.of(false, Collections.EMPTY_MAP);
		}
	}

	private Pair<Boolean, Object> getListValue(String redisKey) {
		try(Jedis jedis = jedisPool.getResource()) {
			Long listLength = jedis.llen(redisKey);
			if(0 != listLength) {
				return Pair.of(true, jedis.lrange(redisKey, 0L, listLength-1).toArray(new String[0]));
			} else {
				return Pair.of(false, new String[0]);
			}
		} catch (Exception e) {
			LOG.error("query redis failed.", e);
			return Pair.of(false, new String[0]);
		}
	}

	private Pair<Boolean, Object> getSetValue(String redisKey) {
		try(Jedis jedis = jedisPool.getResource()) {
			String[] set = jedis.smembers(redisKey).toArray(new String[0]);
			return Pair.of(set.length == 0 ? false : true, set);
		} catch (Exception e) {
			LOG.error("query redis failed.", e);
			return Pair.of(false, new String[0]);
		}
	}

	private Pair<Boolean, Object> getZSetValue(String redisKey) {
		try(Jedis jedis = jedisPool.getResource()) {
			Long length = jedis.zcard(redisKey);
			if(0 != length) {
				return Pair.of(true, jedis.zrange(redisKey, 0, length-1).toArray(new String[0]));
			} else {
				return Pair.of(false, new String[0]);
			}
		} catch (Exception e) {
			LOG.error("query redis failed.", e);
			return Pair.of(false, new String[0]);
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private String ip;

		private int port;

		private String redisPassword;

		private String redisValueType;

		private boolean openCache;

		private int cacheMaxSize = -1;

		private String[] fieldNames;

		private DataType[] fieldTypes;

		private String[] redisJoinKeys;

		private String keyAppendCharacter;

		private boolean writeOpen;

		private RedisWriteStrategy redisWriteStrategy;

		private Builder() {}

		public Builder setIp(String ip) {
			this.ip = ip;
			return this;
		}

		public Builder setPort(int port) {
			this.port = port;
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

		public Builder setFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		public Builder setFieldTypes(DataType[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return this;
		}

		public Builder setRedisJoinKeys(String[] redisJoinKeys) {
			this.redisJoinKeys = redisJoinKeys;
			return this;
		}

		public Builder setOpenCache(boolean openCache) {
			this.openCache = openCache;
			return this;
		}

		public Builder setCacheMaxSize(int cacheMaxSize) {
			this.cacheMaxSize = cacheMaxSize;
			return this;
		}

		public Builder setKeyAppendCharacter(String keyAppendCharacter) {
			this.keyAppendCharacter = keyAppendCharacter;
			return this;
		}

		public Builder setWriteOpen(boolean writeOpen) {
			this.writeOpen = writeOpen;
			return this;
		}

		public Builder setRedisWriteStrategy(RedisWriteStrategy redisWriteStrategy) {
			this.redisWriteStrategy = redisWriteStrategy;
			return this;
		}

		public RedisLookupFunction build() {
			//TODO do check
			return new RedisLookupFunction(ip, port, redisPassword, fieldNames, fieldTypes, redisJoinKeys, redisValueType,
				openCache, cacheMaxSize, keyAppendCharacter, writeOpen, redisWriteStrategy);
		}
	}
}
