package org.apache.flink.table.descriptors;

import org.apache.flink.formats.protobuf.typeutils.MessageVersion;

public class ProtobufValidator extends FormatDescriptorValidator {
    public static final String FORMAT_TYPE_VALUE = "protobuf";
    public static final String FORMAT_SCHEMA = "format.schema";
    public static final String FORMAT_MESSAGE_VERSION = "format.message-version";
    public static final String FORMAT_IGNORE_PARSE_ERRORS = "format.ignore-parse-errors";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateBoolean(FORMAT_DERIVE_SCHEMA, true);
        final boolean deriveSchema =
                properties.getOptionalBoolean(FORMAT_DERIVE_SCHEMA).orElse(true);
        properties.validateType(FORMAT_SCHEMA, deriveSchema, true);
        properties.validateBoolean(FORMAT_IGNORE_PARSE_ERRORS, true);
        properties.validateEnumValues(FORMAT_MESSAGE_VERSION, false, MessageVersion.getVersions());
    }
}
