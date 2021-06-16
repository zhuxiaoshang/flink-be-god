package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.protobuf.typeutils.MessageVersion;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.formats.protobuf.ProtoBufOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.protobuf.ProtoBufOptions.MESSAGE_VERSION;

public class ProtoBufFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "protobuf";

    /**
     * Creates a format from the given context and format options.
     *
     * <p>
     *
     * <p>The format options have been projected to top-level options (e.g. from {@code
     * key.format.ignore-errors} to {@code format.ignore-errors}).
     *
     * @param context
     * @param formatOptions
     */
    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return null;
    }

    /**
     * Creates a format from the given context and format options.
     *
     * <p>
     *
     * <p>The format options have been projected to top-level options (e.g. from {@code
     * key.format.ignore-errors} to {@code format.ignore-errors}).
     *
     * @param context
     * @param formatOptions
     */
    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateFormatOptions(formatOptions);
        Boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        String messageVersion = formatOptions.get(MESSAGE_VERSION);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType producedDataType) {
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo =
                        (TypeInformation<RowData>) context.createTypeInformation(producedDataType);
                return new ProtoBufRowDataDeserializationSchema(
                        rowType, rowDataTypeInfo, messageVersion, ignoreParseErrors);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    /**
     * Returns a unique identifier among same factory interfaces.
     *
     * <p>
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code
     * kafka}). If multiple factories exist for different versions, a version should be appended
     * using "-" (e.g. {@code kafka-0.10}).
     */
    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    /**
     * Returns a set of {@link ConfigOption} that an implementation of this factory requires in
     * addition to {@link #optionalOptions()}.
     *
     * <p>
     *
     * <p>See the documentation of {@link Factory} for more information.
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MESSAGE_VERSION);
        return options;
    }

    /**
     * Returns a set of {@link ConfigOption} that an implementation of this factory consumes in
     * addition to {@link #requiredOptions()}.
     *
     * <p>
     *
     * <p>See the documentation of {@link Factory} for more information.
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IGNORE_PARSE_ERRORS);
        return options;
    }

    static void validateFormatOptions(ReadableConfig tableOptions) {
        String messageVersion = tableOptions.get(MESSAGE_VERSION);
        if (!MessageVersion.getVersions().contains(messageVersion)) {
            throw new ValidationException(
                    String.format(
                            "Unsupported value '%s' for '%s'. Supported values are %s",
                            messageVersion, MESSAGE_VERSION.key(), MessageVersion.getVersions()));
        }
    }
}
