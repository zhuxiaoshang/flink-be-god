package sql.connector.customized;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.Schema.*;
import static sql.connector.customized.CustomizedConnectorDescriptorValidator.*;

public class CustomizedTableSourceSinkFactory implements StreamTableSinkFactory<Row>, StreamTableSourceFactory<Row> {
    /**
     * Creates and configures a {@link StreamTableSink} using the given properties.
     *
     * @param properties normalized properties describing a table sink.
     * @return the configured table sink.
     */
    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        final TableSchema schema = TableSchemaUtils.getPhysicalSchema(
                descriptorProperties.getTableSchema(SCHEMA));
        String job = descriptorProperties.getString(CONNECTOR_JOB);
        String metrics = descriptorProperties.getString(CONNECTOR_METRICS);
        String address = descriptorProperties.getString(CONNECTOR_ADDRESS);
        return new CustomizedTableSink(job, metrics, address, schema);
    }

    /**
     * Only create stream table sink.
     *
     * @param properties
     */
    @Override
    public TableSink<Row> createTableSink(Map<String, String> properties) {
        return null;
    }

    /**
     * Creates and configures a {@link StreamTableSource} using the given properties.
     *
     * @param properties normalized properties describing a stream table source.
     * @return the configured stream table source.
     */
    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        return null;
    }

    /**
     * Only create a stream table source.
     *
     * @param properties
     */
    @Override
    public TableSource<Row> createTableSource(Map<String, String> properties) {
        return null;
    }

    /**
     * Specifies the context that this factory has been implemented for. The framework guarantees to
     * only match for this factory if the specified set of properties and values are met.
     *
     * <p>Typical properties might be:
     * - connector.type
     * - format.type
     *
     * <p>Specified property versions allow the framework to provide backwards compatible properties
     * in case of string format changes:
     * - connector.property-version
     * - format.property-version
     *
     * <p>An empty context means that the factory matches for all requests.
     */
    @Override
    public Map<String, String> requiredContext() {
        /**
         * 这里是connector类型，通过这个配置flink有且只能discover一种connector
         */
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_CUSTOMIZE);
        return context;
    }

    /**
     * List of property keys that this factory can handle. This method will be used for validation.
     * If a property is passed that this factory cannot handle, an exception will be thrown. The
     * list must not contain the keys that are specified by the context.
     *
     * <p>Example properties might be:
     * - schema.#.type
     * - schema.#.name
     * - connector.topic
     * - format.line-delimiter
     * - format.ignore-parse-errors
     * - format.fields.#.type
     * - format.fields.#.name
     *
     * <p>Note: Use "#" to denote an array of values where "#" represents one or more digits. Property
     * versions like "format.property-version" must not be part of the supported properties.
     *
     * <p>In some cases it might be useful to declare wildcards "*". Wildcards can only be declared at
     * the end of a property key.
     *
     * <p>For example, if an arbitrary format should be supported:
     * - format.*
     *
     * <p>Note: Wildcards should be used with caution as they might swallow unsupported properties
     * and thus might lead to undesired behavior.
     */
    @Override
    public List<String> supportedProperties() {
        /**
         * 这里是自定义connector支持的所有配置，尤其注意schema
         */
        List<String> properties = new ArrayList<>();
        properties.add(CONNECTOR_JOB);
        properties.add(CONNECTOR_METRICS);
        properties.add(CONNECTOR_ADDRESS);
        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        return properties;    }
}
