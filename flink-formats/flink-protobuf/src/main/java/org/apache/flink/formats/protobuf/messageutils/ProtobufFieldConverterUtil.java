package org.apache.flink.formats.protobuf.messageutils;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.protobuf.exception.ProtobufException;
import org.apache.flink.formats.protobuf.typeutils.MessageVersion;

public class ProtobufFieldConverterUtil {
    /**
     * initialize An protobuf schema converter (as a {@link ProtobufFieldConverter} ) for the {@link
     * MessageVersion } defined by customer.
     *
     * @return Subclass of {@link ProtobufFieldConverter}
     */
    public static ProtobufFieldConverter genericFieldConverter(
            String messageVersion, RowTypeInfo rowTypeInfo, boolean ignoreParseErrors) {
        switch (MessageVersion.of(messageVersion)) {
            case MESSAGE_V2:
                return new NewMessageV3FieldConverter(rowTypeInfo, ignoreParseErrors);
            case MESSAGE_V1:
                return new MessageFieldConverter(rowTypeInfo, ignoreParseErrors);
            case MESSAGE_V1_ETL:
                return new MessageFieldEtlConverter(rowTypeInfo, ignoreParseErrors);
            case MESSAGE_V1_DATAX:
                return new MessageFieldDataXConverter(rowTypeInfo, ignoreParseErrors);
            case MESSAGE_SIDECAR_LOG:
                return new MessageSidecarLogFieldConverter(rowTypeInfo, ignoreParseErrors);
            default:
                throw new ProtobufException(
                        String.format(
                                "No suitable protobuf schema parser found for '%s' ",
                                messageVersion));
        }
    }
}
