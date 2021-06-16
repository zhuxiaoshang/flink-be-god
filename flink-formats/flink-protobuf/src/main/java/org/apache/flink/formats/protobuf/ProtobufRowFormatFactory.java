package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.protobuf.typeutils.TypeInformationUtil;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.ProtobufValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA;
import static org.apache.flink.table.descriptors.ProtobufValidator.*;

public class ProtobufRowFormatFactory extends TableFormatFactoryBase<Row>
        implements DeserializationSchemaFactory<Row>, SerializationSchemaFactory<Row> {

    /**
     * Creates and configures a [[SerializationSchema]] using the given properties.
     *
     * @param properties normalized properties describing the format
     * @return the configured serialization schema or null if the factory cannot provide an instance
     *     of this class
     */
    @Override
    public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
        return null;
    }

    /**
     * Creates and configures a {@link DeserializationSchema} using the given properties.
     *
     * @param properties normalized properties describing the format
     * @return the configured serialization schema or null if the factory cannot provide an instance
     *     of this class
     */
    @Override
    public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        ProtoBufRowDeserializationSchema.Builder builder =
                ProtoBufRowDeserializationSchema.builder();
        builder.setTypeInfo(createTypeInformation(descriptorProperties));
        builder.setMessageVersion(descriptorProperties.getString(FORMAT_MESSAGE_VERSION));
        descriptorProperties
                .getOptionalBoolean(FORMAT_IGNORE_PARSE_ERRORS)
                .ifPresent(builder::setIgnoreParseErrors);
        return builder.build();
    }

    public ProtobufRowFormatFactory() {
        super(FORMAT_TYPE_VALUE, 1, true);
    }

    /**
     * Format specific context.
     *
     * <p>
     *
     * <p>This method can be used if format type and a property version is not enough.
     */
    @Override
    protected Map<String, String> requiredFormatContext() {
        return super.requiredFormatContext();
    }

    /**
     * Format specific supported properties.
     *
     * <p>
     *
     * <p>This method can be used if schema derivation is not enough.
     */
    @Override
    protected List<String> supportedFormatProperties() {
        final List<String> properties = new ArrayList<>();
        properties.add(ProtobufValidator.FORMAT_MESSAGE_VERSION);
        properties.add(ProtobufValidator.FORMAT_IGNORE_PARSE_ERRORS);
        properties.add(ProtobufValidator.FORMAT_SCHEMA);
        properties.add(ProtobufValidator.FORMAT_DERIVE_SCHEMA);
        return properties;
    }

    private TypeInformation<Row> createTypeInformation(DescriptorProperties descriptorProperties) {
        //		TableSchema schema =
        // TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));
        //		TypeInformation<?>[] fieldTypes = schema.getFieldTypes();
        //		String[] fieldNames = schema.getFieldNames();
        //		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        //		return rowTypeInfo ;
        final boolean deriveSchema =
                descriptorProperties.getOptionalBoolean(FORMAT_DERIVE_SCHEMA).orElse(true);
        final String messageVersion = descriptorProperties.getString(FORMAT_MESSAGE_VERSION);
        if (deriveSchema) {
            return TypeInformationUtil.of(messageVersion);
        } else {
            return (RowTypeInfo) descriptorProperties.getType(ProtobufValidator.FORMAT_SCHEMA);
        }
    }

    private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(propertiesMap);
        // validate
        new ProtobufValidator().validate(descriptorProperties);

        return descriptorProperties;
    }
}
