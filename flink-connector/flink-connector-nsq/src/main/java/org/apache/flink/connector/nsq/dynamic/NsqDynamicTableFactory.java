package org.apache.flink.connector.nsq.dynamic;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

public class NsqDynamicTableFactory implements DynamicTableSinkFactory {
    public static final ConfigOption<String> NSQ_IP =
            ConfigOptions.key("ip")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the nsq's connect address.");
    public static final ConfigOption<Integer> NSQ_PORT =
            ConfigOptions.key("port")
                    .intType()
                    .noDefaultValue()
                    .withDescription("the nsq's connect port.");
    public static final ConfigOption<String> NSQ_TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the nsq's write topic.");
    public static final ConfigOption<Integer> PARALLELISM =
            ConfigOptions.key("parallelism")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("the parallelism writing to nsq.");

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
        return NsqDynamicTableSink.builder()
                .setNsqIp(tableOptions.get(NSQ_IP))
                .setNsqPort(tableOptions.get(NSQ_PORT))
                .setTopic(tableOptions.get(NSQ_TOPIC))
                .setParallelism(tableOptions.get(PARALLELISM))
                .setTableSchema(context.getCatalogTable().getSchema())
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
        return "org/apache/flink/connector/nsq";
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
        options.add(NSQ_IP);
        options.add(NSQ_PORT);
        options.add(NSQ_TOPIC);
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
        options.add(PARALLELISM);
        return options;
    }
}
