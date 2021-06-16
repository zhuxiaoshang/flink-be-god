package org.apache.flink.formats.protobuf.messageutils;

import com.google.protobuf.Descriptors;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.protobuf.exception.ProtobufException;
import org.apache.flink.formats.protobuf.message.Message;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageFieldDataXConverter extends ProtobufFieldConverter {

    public MessageFieldDataXConverter(RowTypeInfo typeInformation, Boolean ignoreParseErrors) {
        super(typeInformation, ignoreParseErrors);
    }

    @Override
    String initializeSchemaString() {
        return "package pbdata;\n"
                + " \n"
                + "message ValueType {\n"
                + "    optional int32 int_type =1;\n"
                + "    optional int64 long_type = 2;\n"
                + "    optional float float_type = 3;\n"
                + "    optional string string_type =4;\n"
                + "}\n"
                + " \n"
                + "message Log {\n"
                + "    required int64 log_timestamp =1;\n"
                + "    optional string ip = 2;\n"
                + "    optional group Field  = 3 {\n"
                + "        repeated group Map = 1 {\n"
                + "            required string key = 1;\n"
                + "            optional ValueType value = 2;\n"
                + "        }\n"
                + "    }\n"
                + "}";
    }

    @Override
    Map<String, Descriptors.FieldDescriptor> initializeFieldDescriptors() {
        List<Descriptors.FieldDescriptor> fields = Message.Log.getDescriptor().getFields();
        Map<String, Descriptors.FieldDescriptor> descriptorMap = new HashMap<>(fields.size());
        for (Descriptors.FieldDescriptor field : fields) {
            String name = field.toProto().getName();
            descriptorMap.put(name, field);
        }
        return descriptorMap;
    }

    @Override
    Row convertSchemaToRow(byte[] message) {
        return null;
    }

    @Override
    Object[] convertSchemaToObjectArray(byte[] message) {
        return createFieldValues(message);
    }

    private Object[] createFieldValues(byte[] message) {
        try {
            Message.Log log = Message.Log.parseFrom(message);
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

    private Object getFieldValue(Message.Log log, int columnNumber) throws Exception {
        if (log == null) {
            return null;
        }
        switch (columnNumber) {
            case Message.Log.LOG_TIMESTAMP_FIELD_NUMBER:
                return log.getLogTimestamp();
            case Message.Log.IP_FIELD_NUMBER:
                return log.getIp();
            case Message.Log.FIELD_FIELD_NUMBER:
                return createRowFromField(log.getField());
            default:
                return null;
        }
    }

    private Map<String, Row> createRowFromField(Message.Log.Field field) {
        HashMap<String, Row> resultMap = new HashMap<>();
        if (field != null) {
            for (Message.Log.Field.Map map : field.getMapList()) {
                Message.ValueType mapValue = map.getValue();
                String key = map.getKey();
                Row row = new Row(4);
                row.setField(0, mapValue.hasIntType() ? mapValue.getIntType() : null);
                row.setField(1, mapValue.hasLongType() ? mapValue.getLongType() : null);
                row.setField(2, mapValue.hasFloatType() ? mapValue.getFloatType() : null);
                row.setField(3, mapValue.hasStringType() ? mapValue.getStringType() : null);
                resultMap.put(key, row);
            }
        }
        return resultMap;
    }
}
