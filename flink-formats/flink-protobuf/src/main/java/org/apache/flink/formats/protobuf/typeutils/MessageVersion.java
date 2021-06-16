package org.apache.flink.formats.protobuf.typeutils;

import java.util.ArrayList;
import java.util.List;

public enum MessageVersion {
    MESSAGE_V1("message-v1", "org.apache.flink.formats.protobuf.message.Message$Log"),
    MESSAGE_V2("message-v2", "org.apache.flink.formats.protobuf.message.NewMessageV3$Log"),
    MESSAGE_V1_ETL("message-v1-etl", "org.apache.flink.formats.protobuf.message.Message$Log"),
    MESSAGE_V1_DATAX("message-v1-datax", "org.apache.flink.formats.protobuf.message.Message$Log"),
    MESSAGE_SIDECAR_LOG(
            "message-sidecar-log",
            "org.apache.flink.formats.protobuf.message.MessageSidecarLog$Log");

    /** Identity the version corresponding to the protobuf file */
    private String version;

    /** Identify the java class corresponding to the protobuf file */
    private String fullName;

    public String getVersion() {
        return version;
    }

    public String getFullName() {
        return fullName;
    }

    private MessageVersion(String version, String clazzName) {
        this.version = version;
        this.fullName = clazzName;
    }

    public static List<String> getVersions() {
        MessageVersion[] values = MessageVersion.values();
        List<String> messageVersions = new ArrayList<>(values.length);
        for (int i = 0; i < values.length; i++) {
            messageVersions.add(values[i].version);
        }
        return messageVersions;
    }

    public static MessageVersion of(String version) {
        for (MessageVersion mv : MessageVersion.values()) {
            if (mv.version.equals(version)) {
                return mv;
            }
        }
        return null;
    }

    public static String findFullName(String version) {
        for (MessageVersion mv : MessageVersion.values()) {
            if (mv.version.equals(version)) {
                return mv.fullName;
            }
        }
        return null;
    }
}
