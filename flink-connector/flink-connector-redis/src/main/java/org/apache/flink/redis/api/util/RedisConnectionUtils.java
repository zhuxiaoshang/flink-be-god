package org.apache.flink.redis.api.util;

import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisConnectionUtils {

	private static final int TIMEOUT = 2000;

	public static JedisPoolConfig getJedisPoolConfig() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(5);
		config.setMaxWaitMillis(1000L);
		config.setTestOnBorrow(false);
		return config;
	}

	public static JedisPool getJedisPool(JedisPoolConfig config, String ip, int port, String redisPassword) {
		if(StringUtils.isBlank(redisPassword)) {
			return new JedisPool(config, ip ,port);
		} else {
			return new JedisPool(config, ip ,port, TIMEOUT, redisPassword);
		}

	}

	public static void closeJedisPool(JedisPool jedisPool) {
		if(null != jedisPool && !jedisPool.isClosed()) {
			jedisPool.close();
		}
	}
}
