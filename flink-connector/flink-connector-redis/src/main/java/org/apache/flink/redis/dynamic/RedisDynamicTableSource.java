package org.apache.flink.redis.dynamic;

import org.apache.flink.redis.api.RedisLookupFunction;
import org.apache.flink.redis.api.util.DESUtils;
import org.apache.flink.redis.api.util.RedisWriteStrategy;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

/**
 * @author: zhushang
 * @create: 2020-08-27 19:04
 */
public class RedisDynamicTableSource implements LookupTableSource, ScanTableSource {
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

    public RedisDynamicTableSource(
            String[] fieldNames,
            DataType[] fieldTypes,
            String ip,
            int port,
            String password,
            String redisValueType,
            boolean openCache,
            int cacheMaxSize,
            String keyAppendCharacter,
            boolean writeOpen,
            RedisWriteStrategy redisWriteStrategy) {
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
        if (writeOpen
                && RedisWriteStrategy.ALWAYS.getCode().equals(redisWriteStrategy)
                && openCache) {
            throw new FlinkRuntimeException("when key.strategy is 'always',should not open cache.");
        }
    }

    /**
     * Returns a provider of runtime implementation for reading the data.
     *
     * <p>There exist different interfaces for runtime implementation which is why {@link
     * LookupRuntimeProvider} serves as the base interface.
     *
     * <p>Independent of the provider interface, a source implementation can work on either
     * arbitrary objects or internal data structures (see {@link org.apache.flink.table.data} for
     * more information).
     *
     * <p>The given {@link LookupContext} offers utilities by the planner for creating runtime
     * implementation with minimal dependencies to internal data structures.
     *
     * @param context
     * @see TableFunctionProvider
     * @see AsyncTableFunctionProvider
     */
    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "redis only support non-nested look up keys");
            keyNames[i] = fieldNames[innerKeyArr[0]];
        }
        RedisLookupFunction lookupFunction =
                RedisLookupFunction.builder()
                        .setIp(ip)
                        .setPort(port)
                        .setRedisPassword(DESUtils.decryptFromBase64(redisPassword))
                        .setRedisValueType(redisValueType)
                        .setFieldNames(fieldNames)
                        .setFieldTypes(fieldTypes)
                        .setRedisJoinKeys(keyNames)
                        .setCacheMaxSize(cacheMaxSize)
                        .setOpenCache(openCache)
                        .setKeyAppendCharacter(keyAppendCharacter)
                        .setWriteOpen(writeOpen)
                        .setRedisWriteStrategy(redisWriteStrategy)
                        .build();
        return TableFunctionProvider.of(lookupFunction);
    }

    /**
     * Creates a copy of this instance during planning. The copy should be a deep copy of all
     * mutable members.
     */
    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(
                fieldNames,
                fieldTypes,
                ip,
                port,
                redisPassword,
                redisValueType,
                openCache,
                cacheMaxSize,
                keyAppendCharacter,
                writeOpen,
                redisWriteStrategy);
    }

    /** Returns a string that summarizes this source for printing to a console or log. */
    @Override
    public String asSummaryString() {
        return "redis source";
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the set of changes that the planner can expect during runtime.
     *
     * @see RowKind
     */
    @Override
    public ChangelogMode getChangelogMode() {

        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    /**
     * Returns a provider of runtime implementation for reading the data.
     *
     * <p>There might exist different interfaces for runtime implementation which is why {@link
     * ScanRuntimeProvider} serves as the base interface. Concrete {@link ScanRuntimeProvider}
     * interfaces might be located in other Flink modules.
     *
     * <p>Independent of the provider interface, the table runtime expects that a source
     * implementation emits internal data structures (see {@link RowData} for more information).
     *
     * <p>The given {@link ScanContext} offers utilities by the planner for creating runtime
     * implementation with minimal dependencies to internal data structures.
     *
     * <p>See {@code org.apache.flink.table.connector.source.SourceFunctionProvider} in {@code
     * flink-table-api-java-bridge}.
     *
     * @param runtimeProviderContext
     */
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        // currently unsupported
        return null;
    }

    public static class Builder {
        private String[] fieldNames;

        private DataType[] fieldTypes;

        private String ip;

        private int port;

        private String redisPassword;

        private String redisValueType;

        private boolean openCache;

        private int cacheMaxSize;

        private String keyAppendCharacter;

        private boolean writeOpen;

        private RedisWriteStrategy redisWriteStrategy;

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldTypes(DataType[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

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

        public RedisDynamicTableSource build() {
            return new RedisDynamicTableSource(
                    fieldNames,
                    fieldTypes,
                    ip,
                    port,
                    redisPassword,
                    redisValueType,
                    openCache,
                    cacheMaxSize,
                    keyAppendCharacter,
                    writeOpen,
                    redisWriteStrategy);
        }
    }
}
