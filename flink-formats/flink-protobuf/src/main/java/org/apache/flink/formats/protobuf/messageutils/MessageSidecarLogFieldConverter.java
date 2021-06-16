package org.apache.flink.formats.protobuf.messageutils;

import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.protobuf.exception.ProtobufException;
import org.apache.flink.formats.protobuf.message.MessageSidecarLog.Log;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MessageSidecarLogFieldConverter extends ProtobufFieldConverter {

    public MessageSidecarLogFieldConverter(RowTypeInfo typeInformation, Boolean ignoreParseErrors) {
        super(typeInformation, ignoreParseErrors);
    }

    @Override
    String initializeSchemaString() {
        return "syntax = \"proto3\";\n"
                + "\n"
                + "message Log {\n"
                + "  string app = 1;                       // 服务简短名称\n"
                + "  string opsservice = 2;                // PaasID\n"
                + "  string container_ip = 3;              // 容器IP, ecs为项目IP\n"
                + "  string container_name = 4;            // 容器名称, ecs为空\n"
                + "  string node_ip = 5;                   // node_ip, ecs为空\n"
                + "  string hostname = 6;                  // hostname\n"
                + "  string image_name = 7;                // 镜像, ecs为空\n"
                + "  string namespace = 8;                 // 命名空间, ecs为空\n"
                + "  string pod_name = 9;                  // pod名称, ecs为空\n"
                + "  string source = 10;                    // 文件名\n"
                + "  string content = 11;                   // 内容\n"
                + "  int64 timestamp = 12;                  // 发送时间纳秒\n"
                + "}\n";
    }

    @Override
    Map<String, FieldDescriptor> initializeFieldDescriptors() {
        List<FieldDescriptor> fields = Log.getDescriptor().getFields();
        return fields.stream().collect(Collectors.toMap(f -> f.toProto().getName(), f -> f));
    }

    @Override
    Row convertSchemaToRow(byte[] message) {
        return null;
    }

    @Override
    Object[] convertSchemaToObjectArray(byte[] message) {
        try {
            Log log = Log.parseFrom(message);
            Object[] objects = new Object[protoFieldNumbers.length];
            for (int i = 0; i < this.protoFieldNumbers.length; i++) {
                objects[i] = getFieldValue(log, protoFieldNumbers[i]);
            }
            return objects;
        } catch (Exception e) {
            if (!ignoreParseErrors) {
                throw new ProtobufException("parse row exception ", e);
            }
        }
        return null;
    }

    private Object getFieldValue(Log log, int protoFieldNumber) {
        if (log == null) {
            return null;
        }
        switch (protoFieldNumber) {
            case Log.APP_FIELD_NUMBER:
                return log.hasApp() ? log.getApp() : null;
            case Log.OPSSERVICE_FIELD_NUMBER:
                return log.hasOpsservice() ? log.getOpsservice() : null;
            case Log.CONTAINER_IP_FIELD_NUMBER:
                return log.hasContainerIp() ? log.getContainerIp() : null;
            case Log.CONTAINER_NAME_FIELD_NUMBER:
                return log.hasContainerName() ? log.getContainerName() : null;
            case Log.NODE_IP_FIELD_NUMBER:
                return log.hasNodeIp() ? log.getNodeIp() : null;
            case Log.HOSTNAME_FIELD_NUMBER:
                return log.hasHostname() ? log.getHostname() : null;
            case Log.IMAGE_NAME_FIELD_NUMBER:
                return log.hasImageName() ? log.getImageName() : null;
            case Log.NAMESPACE_FIELD_NUMBER:
                return log.hasNamespace() ? log.getNamespace() : null;
            case Log.POD_NAME_FIELD_NUMBER:
                return log.hasPodName() ? log.getPodName() : null;
            case Log.SOURCE_FIELD_NUMBER:
                return log.hasSource() ? log.getSource() : null;
            case Log.CONTENT_FIELD_NUMBER:
                return log.hasContent() ? log.getContent() : null;
            case Log.TIMESTAMP_FIELD_NUMBER:
                return log.hasTimestamp() ? log.getTimestamp() : null;
            default:
                return null;
        }
    }
}
