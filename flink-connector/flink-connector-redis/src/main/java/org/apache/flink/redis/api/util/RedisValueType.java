package org.apache.flink.redis.api.util;

public enum RedisValueType {
    REDIS_VALUE_TYPE_STRING("string"),
    REDIS_VALUE_TYPE_HASH("hash"),
    REDIS_VALUE_TYPE_LIST("list"),
    REDIS_VALUE_TYPE_SET("set"),
    REDIS_VALUE_TYPE_ZSET("zset");

    private String type;

    RedisValueType(String type) {
        this.type = type;
    }

    public static RedisValueType getRedisValueType(String type) {
        switch (type) {
            case "string":
                return REDIS_VALUE_TYPE_STRING;
            case "hash":
                return REDIS_VALUE_TYPE_HASH;
            case "list":
                return REDIS_VALUE_TYPE_LIST;
            case "set":
                return REDIS_VALUE_TYPE_SET;
            case "zset":
                return REDIS_VALUE_TYPE_ZSET;
            default:
                return REDIS_VALUE_TYPE_STRING;
        }
    }
}
