package org.apache.flink.formats.protobuf.messageutils;

import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.io.Serializable;

public final class ProtobufRowSchemaConverter implements Serializable {

    private static final long serialVersionUID = -228294630688809165L;
    /** Identity the version corresponding to the protobuf file. */
    private final String messageVersion;
    /** Identity the TypeInformation defined by customer. */
    private final RowTypeInfo rowTypeInfo;
    /** */
    private final Boolean ignoreParseErrors;
    /** */
    private final ProtobufFieldConverter fieldConverter;

    private ProtobufRowSchemaConverter(
            String messageVersion, RowTypeInfo rowTypeInfo, Boolean ignoreParseErrors) {
        this.messageVersion = messageVersion;
        this.rowTypeInfo = rowTypeInfo;
        this.ignoreParseErrors = ignoreParseErrors;
        this.fieldConverter =
                ProtobufFieldConverterUtil.genericFieldConverter(
                        messageVersion, rowTypeInfo, ignoreParseErrors);
    }

    /**
     * Deserialize byte array into corresponding object array.
     *
     * @param message Array of bytes to be deserialized.
     * @return Object array in the same order as the user column definition.
     */
    public Object[] concertProtobufMessageToObjects(byte[] message) {
        return fieldConverter.convert(message);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String messageVersion;
        private RowTypeInfo rowTypeInfo;
        private Boolean ignoreParseErrors;

        public Builder setMessageVersion(String messageVersion) {
            this.messageVersion = messageVersion;
            return this;
        }

        public Builder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
            this.rowTypeInfo = rowTypeInfo;
            return this;
        }

        public Builder setIgnoreParseErrors(Boolean ignoreParseErrors) {
            this.ignoreParseErrors = ignoreParseErrors;
            return this;
        }

        public ProtobufRowSchemaConverter build() {
            ProtobufRowSchemaConverter converter =
                    new ProtobufRowSchemaConverter(messageVersion, rowTypeInfo, ignoreParseErrors);
            return converter;
        }
    }
}
