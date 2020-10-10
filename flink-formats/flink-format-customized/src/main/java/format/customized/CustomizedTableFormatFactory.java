package format.customized;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableFormatFactory;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

/**
 * @author: zhushang
 * @create: 2020-10-05 16:19
 **/

public class CustomizedTableFormatFactory implements TableFormatFactory<Row>,
        SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row> {
    /**
     * Flag to indicate if the given format supports deriving information from a schema. If the
     * format can handle schema information, those properties must be added to the list of
     * supported properties.
     */
    @Override
    public boolean supportsSchemaDerivation() {
        return false;
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
        return null;
    }

    /**
     * List of format property keys that this factory can handle. This method will be used for
     * validation. If a property is passed that this factory cannot handle, an exception will be
     * thrown. The list must not contain the keys that are specified by the context.
     *
     * <p>Example format properties might be:
     * - format.line-delimiter
     * - format.ignore-parse-errors
     * - format.fields.#.type
     * - format.fields.#.name
     *
     * <p>If schema derivation is enabled, the list must include schema properties:
     * - schema.#.name
     * - schema.#.type
     *
     * <p>Note: All supported format properties must be prefixed with "format.". If schema derivation is
     * enabled, also properties with "schema." prefix can be used.
     *
     * <p>Use "#" to denote an array of values where "#" represents one or more digits. Property
     * versions like "format.property-version" must not be part of the supported properties.
     *
     * <p>See also {@link TableFactory#supportedProperties()} for more information.
     */
    @Override
    public List<String> supportedProperties() {
        return null;
    }

    /**
     * Creates and configures a {@link DeserializationSchema} using the given properties.
     *
     * @param properties normalized properties describing the format
     * @return the configured serialization schema or null if the factory cannot provide an
     * instance of this class
     */
    @Override
    public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
        return null;
    }

    /**
     * Creates and configures a [[SerializationSchema]] using the given properties.
     *
     * @param properties normalized properties describing the format
     * @return the configured serialization schema or null if the factory cannot provide an
     * instance of this class
     */
    @Override
    public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
        return null;
    }
}
