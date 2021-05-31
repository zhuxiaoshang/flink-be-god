package org.apache.flink.redis.dynamic;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.redis.api.util.RedisWriteStrategy;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.redis.dynamic.RedisOptions.*;

/**
 * @author: zhushang
 * @create: 2020-08-27 17:30
 */
public class RedisDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    /**
     * Creates a {@link DynamicTableSink} instance from a {@link CatalogTable} and additional
     * context information.
     *
     * <p>An implementation should perform validation and the discovery of further (nested)
     * factories in this method.
     *
     * @param context
     */
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig tableOptions = helper.getOptions();
        helper.validateExcept("No");
        return RedisDynamicTableSink.builder()
                .setRedisIp(tableOptions.get(IP))
                .setRedisPort(tableOptions.get(PORT))
                .setRedisPassword(tableOptions.get(PASSWORD))
                .setRedisValueType(tableOptions.get(TYPE))
                .setTtl(tableOptions.get(TTL))
                .setTableSchema(context.getCatalogTable().getSchema())
                .build();
    }

    /**
     * Creates a {@link DynamicTableSource} instance from a {@link CatalogTable} and additional
     * context information.
     *
     * <p>An implementation should perform validation and the discovery of further (nested)
     * factories in this method.
     *
     * @param context
     */
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig readableConfig = helper.getOptions();
        helper.validateExcept("No");
        return RedisDynamicTableSource.builder()
                .setIp(readableConfig.get(IP))
                .setPort(readableConfig.get(PORT))
                .setRedisPassword(readableConfig.get(PASSWORD))
                .setRedisValueType(readableConfig.get(TYPE))
                .setWriteOpen(readableConfig.get(WRITE_OPEN))
                .setRedisWriteStrategy(
                        RedisWriteStrategy.get(readableConfig.get(STORE_KEY_STRATEGY)))
                .setOpenCache(readableConfig.get(OPEN_CACHE))
                .setKeyAppendCharacter(readableConfig.get(KEY_APPEND_CHARACTER))
                .setCacheMaxSize(readableConfig.get(CACHE_SIZE))
                .setFieldNames(context.getCatalogTable().getSchema().getFieldNames())
                .setFieldTypes(context.getCatalogTable().getSchema().getFieldDataTypes())
                .build();
    }

    /**
     * Returns a unique identifier among same factory interfaces.
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code
     * kafka}). If multiple factories exist for different versions, a version should be appended
     * using "-" (e.g. {@code kafka-0.10}).
     */
    @Override
    public String factoryIdentifier() {
        return "redis";
    }

    /**
     * Returns a set of {@link ConfigOption} that an implementation of this factory requires in
     * addition to {@link #optionalOptions()}.
     *
     * <p>See the documentation of {@link Factory} for more information.
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IP);
        options.add(PORT);
        options.add(PASSWORD);
        options.add(TYPE);
        return options;
    }

    /**
     * Returns a set of {@link ConfigOption} that an implementation of this factory consumes in
     * addition to {@link #requiredOptions()}.
     *
     * <p>See the documentation of {@link Factory} for more information.
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(WRITE_OPEN);
        options.add(STORE_KEY_STRATEGY);
        options.add(KEY_APPEND_CHARACTER);
        options.add(OPEN_CACHE);
        options.add(CACHE_SIZE);
        options.add(TTL);
        return options;
    }
}
