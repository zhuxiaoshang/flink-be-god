package sql.connector.customized;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.*;

import static org.apache.flink.table.descriptors.Schema.*;
import static sql.connector.customized.CustomizedConnectorDescriptorValidator.*;

public class CustomizedTableSourceSinkFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static  final ConfigOption<String> TOPIC = ConfigOptions
            .key("topic")
            .stringType()
            .noDefaultValue()
            .withDescription("Required topic name from which the table is read");
    public static  final ConfigOption<String> JOB = ConfigOptions
            .key(CONNECTOR_JOB)
            .stringType()
            .noDefaultValue()
            .withDescription("Required job from which the table is read");
    public static  final ConfigOption<String> METRICS = ConfigOptions
            .key(CONNECTOR_METRICS)
            .stringType()
            .noDefaultValue()
            .withDescription("Required metrics from which the table is read");
    public static  final ConfigOption<String> ADDRESS = ConfigOptions
            .key(CONNECTOR_ADDRESS)
            .stringType()
            .noDefaultValue()
            .withDescription("Required address from which the table is read");

    /**
     * Creates a {@link DynamicTableSink} instance from a {@link CatalogTable} and additional context
     * information.
     *
     * <p>An implementation should perform validation and the discovery of further (nested) factories
     * in this method.
     *
     * @param context
     */
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(context.getCatalogTable().getOptions());
        final TableSchema schema = TableSchemaUtils.getPhysicalSchema(
                descriptorProperties.getTableSchema(SCHEMA));
        String job = descriptorProperties.getString(CONNECTOR_JOB);
        String metrics = descriptorProperties.getString(CONNECTOR_METRICS);
        String address = descriptorProperties.getString(CONNECTOR_ADDRESS);
        return new CustomizedTableSink(job, metrics, address, schema);
    }

    /**
     * Creates a {@link DynamicTableSource} instance from a {@link CatalogTable} and additional context
     * information.
     *
     * <p>An implementation should perform validation and the discovery of further (nested) factories
     * in this method.
     *
     * @param context
     */
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return null;
    }

    /**
     * Returns a unique identifier among same factory interfaces.
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code kafka}). If
     * multiple factories exist for different versions, a version should be appended using "-" (e.g. {@code kafka-0.10}).
     */
    @Override
    public String factoryIdentifier() {
        return CONNECTOR_TYPE_VALUE_CUSTOMIZE;
    }

    /**
     * Returns a set of {@link ConfigOption} that an implementation of this factory requires in addition to
     * {@link #optionalOptions()}.
     *
     * <p>See the documentation of {@link Factory} for more information.
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(TOPIC);
        return requiredOptions;
    }

    /**
     * Returns a set of {@link ConfigOption} that an implementation of this factory consumes in addition to
     * {@link #requiredOptions()}.
     *
     * <p>See the documentation of {@link Factory} for more information.
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(JOB);
        optionalOptions.add(METRICS);
        optionalOptions.add(ADDRESS);
        return optionalOptions;
    }
}
