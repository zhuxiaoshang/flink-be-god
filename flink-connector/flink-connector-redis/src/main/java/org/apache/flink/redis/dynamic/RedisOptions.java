package org.apache.flink.redis.dynamic;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.redis.api.util.RedisWriteStrategy;

/**
 * @author: zhushang
 * @create: 2020-08-27 17:33
 */
public class RedisOptions {
    public static final ConfigOption<String> IP =
            ConfigOptions.key("ip")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the redis's connect address.");
    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .noDefaultValue()
                    .withDescription("the redis's connect port.");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the redis's connect password.");
    public static final ConfigOption<String> TYPE =
            ConfigOptions.key("type")
                    .stringType()
                    .defaultValue("string")
                    .withDescription("the redis's data type.");
    public static final ConfigOption<Integer> TTL =
            ConfigOptions.key("ttl")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("time to live of the key");
    public static final ConfigOption<Boolean> WRITE_OPEN =
            ConfigOptions.key("write.open")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("weather write to redis when value not exist.");
    public static final ConfigOption<String> STORE_KEY_STRATEGY =
            ConfigOptions.key("store.key.strategy")
                    .stringType()
                    .defaultValue(RedisWriteStrategy.WRITE_IF_NOT_EXIST.getCode())
                    .withDescription(
                            "the strategy when write to redis,write_if_not_exist or always is available.");
    public static final ConfigOption<String> KEY_APPEND_CHARACTER =
            ConfigOptions.key("key.append.character")
                    .stringType()
                    .defaultValue("|")
                    .withDescription("the key append character.");
    public static final ConfigOption<Boolean> OPEN_CACHE =
            ConfigOptions.key("open.cache")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("weather to open cache.");
    public static final ConfigOption<Integer> CACHE_SIZE =
            ConfigOptions.key("cache.size")
                    .intType()
                    .defaultValue(10000000)
                    .withDescription("when open cache,the cache size should be specified.");
}
