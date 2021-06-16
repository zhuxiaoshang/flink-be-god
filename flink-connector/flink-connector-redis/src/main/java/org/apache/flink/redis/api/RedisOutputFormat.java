package org.apache.flink.redis.api;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.redis.api.util.RedisConnectionUtils;
import org.apache.flink.redis.api.util.RedisValueType;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RedisOutputFormat<T> extends RichOutputFormat<T> {
    public static final Logger LOG = LoggerFactory.getLogger(RedisOutputFormat.class);

    private String redisIp;

    private int redisPort;

    private String redisPassword;

    private String redisValueType;

    private RedisValueType type;

    private int ttl;

    private transient JedisPoolConfig jedisPoolConfig;

    private transient JedisPool jedisPool;

    public RedisOutputFormat(
            String redisIp, int redisPort, String redisPassword, String redisValueType, int ttl) {
        this.redisIp = redisIp;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
        this.redisValueType = redisValueType;
        this.ttl = ttl;
        this.type = RedisValueType.getRedisValueType(this.redisValueType);
    }

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        jedisPoolConfig = RedisConnectionUtils.getJedisPoolConfig();
        jedisPool =
                RedisConnectionUtils.getJedisPool(
                        jedisPoolConfig, redisIp, redisPort, redisPassword);
        LOG.info("Initialize redis success.");
    }

    @Override
    public void writeRecord(T record) throws IOException {
        Tuple2<Boolean, Row> tuple;
        RowData rowData;
        if (record instanceof Tuple2) {
            tuple = (Tuple2<Boolean, Row>) record;
            writeRedis(tuple.f1);
        } else if (record instanceof RowData) {
            rowData = (RowData) record;
            writeRedis(rowData);
        } else {
            throw new FlinkRuntimeException("can not handle record : " + record);
        }
    }

    private void writeRedis(RowData record) {
        final RowKind rowKind = record.getRowKind();
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                insert(record);
                break;
            case UPDATE_BEFORE:
                break;
            case DELETE:
                delete(record);
                break;
            default:
                throw new FlinkRuntimeException("unsupport rowkind :" + rowKind);
        }
    }

    private void delete(RowData record) {
        String redisKey = record.getString(0).toString();
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(redisKey);
        }
    }

    private void insert(RowData record) {
        String redisKey = record.getString(0).toString();
        switch (type) {
            case REDIS_VALUE_TYPE_STRING:
                String stringValue = record.getString(1).toString();
                writeStringValue(redisKey, stringValue);
                break;
            case REDIS_VALUE_TYPE_HASH:
                MapData map = record.getMap(1);
                ArrayData keyArray = map.keyArray();
                ArrayData valueArray = map.valueArray();
                Map<String, String> hashValue = new HashMap<>();
                for (int i = 0; i < keyArray.size(); i++) {
                    hashValue.put(
                            keyArray.getString(i).toString(), valueArray.getString(i).toString());
                }
                writeHashValue(redisKey, hashValue);
                break;
            case REDIS_VALUE_TYPE_LIST:
                ArrayData array = record.getArray(1);
                String[] listValue = new String[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    listValue[i] = array.getString(i).toString();
                }
                writeListValue(redisKey, listValue);
                break;
            case REDIS_VALUE_TYPE_SET:
                ArrayData arraySet = record.getArray(1);
                String[] setValue = new String[arraySet.size()];
                for (int i = 0; i < arraySet.size(); i++) {
                    setValue[i] = arraySet.getString(i).toString();
                }
                writeListValue(redisKey, setValue);
                break;
            case REDIS_VALUE_TYPE_ZSET:
                throw new RuntimeException("value type [sorted set] unsupport now.");
            default:
                throw new RuntimeException(String.format("unknow redis value type [%s]", type));
        }
    }

    private void writeRedis(Row record) {
        String redisKey = String.valueOf(record.getField(0));
        switch (type) {
            case REDIS_VALUE_TYPE_STRING:
                String stringValue = (String) record.getField(1);
                writeStringValue(redisKey, stringValue);
                break;
            case REDIS_VALUE_TYPE_HASH:
                Map<String, String> hashValue = (Map<String, String>) record.getField(1);
                writeHashValue(redisKey, hashValue);
                break;
            case REDIS_VALUE_TYPE_LIST:
                String[] listValue = (String[]) record.getField(1);
                writeListValue(redisKey, listValue);
                break;
            case REDIS_VALUE_TYPE_SET:
                String[] setValue = (String[]) record.getField(1);
                writeSetValue(redisKey, setValue);
                break;
            case REDIS_VALUE_TYPE_ZSET:
                throw new RuntimeException("value type [sorted set] unsupport now.");
            default:
                throw new RuntimeException(String.format("unknow redis value type [%s]", type));
        }
    }

    private void writeStringValue(String redisKey, String stringValue) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(redisKey, stringValue);
            if (ttl > 0) {
                jedis.expire(redisKey, ttl);
            }
        }
    }

    private void writeHashValue(String redisKey, Map<String, String> hashValue) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hmset(redisKey, hashValue);
            if (ttl > 0) {
                jedis.expire(redisKey, ttl);
            }
        }
    }

    private void writeListValue(String redisKey, String[] listValue) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.lpush(redisKey, listValue);
            if (ttl > 0) {
                jedis.expire(redisKey, ttl);
            }
        }
    }

    private void writeSetValue(String redisKey, String[] setValue) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.sadd(redisKey, setValue);
            if (ttl > 0) {
                jedis.expire(redisKey, ttl);
            }
        }
    }

    private void writeZSetValue() {}

    @Override
    public void close() throws IOException {
        RedisConnectionUtils.closeJedisPool(jedisPool);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** RedisOutputFormat 的Builder */
    public static class Builder {

        private String redisIp;

        private int redisPort;

        private String redisPassword;

        private String redisValueType;

        private int ttl;

        public Builder setRedisIp(String redisIp) {
            this.redisIp = redisIp;
            return this;
        }

        public Builder setRedisPort(int redisPort) {
            this.redisPort = redisPort;
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

        public Builder setTtl(int ttl) {
            this.ttl = ttl;
            return this;
        }

        public RedisOutputFormat build() {
            return new RedisOutputFormat(redisIp, redisPort, redisPassword, redisValueType, ttl);
        }
    }
}
