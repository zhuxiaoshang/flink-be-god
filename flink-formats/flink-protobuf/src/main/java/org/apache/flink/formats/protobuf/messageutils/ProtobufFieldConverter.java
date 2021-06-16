package org.apache.flink.formats.protobuf.messageutils;

import com.google.protobuf.Descriptors;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.protobuf.exception.ProtobufException;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class ProtobufFieldConverter implements Serializable {

    private static final long serialVersionUID = -228294630688809195L;
    protected static Map<String, Descriptors.FieldDescriptor> FIELD_DESCRIPTORS = new HashMap<>();
    protected static String PROTOBUF_SCHEMA = "No schema";
    protected final RowTypeInfo typeInformation;
    protected final Boolean ignoreParseErrors;
    protected Integer[] protoFieldNumbers;
    protected Descriptors.FieldDescriptor.JavaType[] protoFieldJavaTypes;

    public ProtobufFieldConverter(RowTypeInfo typeInformation, Boolean ignoreParseErrors) {
        this.typeInformation = typeInformation;
        this.ignoreParseErrors = ignoreParseErrors;
        this.PROTOBUF_SCHEMA = initializeSchemaString();
        this.FIELD_DESCRIPTORS = initializeFieldDescriptors();
        validateTypeInformation();
    }

    abstract String initializeSchemaString();

    abstract Map<String, Descriptors.FieldDescriptor> initializeFieldDescriptors();

    public final Row convertToRow(byte[] message) {
        return convertSchemaToRow(message);
    }

    abstract Row convertSchemaToRow(byte[] message);

    public final Object[] convert(byte[] message) {
        return convertSchemaToObjectArray(message);
    }

    abstract Object[] convertSchemaToObjectArray(byte[] message);

    private void validateTypeInformation() {
        String[] fieldNames = typeInformation.getFieldNames();
        Integer[] numbers = new Integer[fieldNames.length];
        Descriptors.FieldDescriptor.JavaType[] javaTypes =
                new Descriptors.FieldDescriptor.JavaType[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = fieldNames[i];
            Descriptors.FieldDescriptor fieldDescriptor = FIELD_DESCRIPTORS.get(fieldName);
            if (fieldDescriptor == null) {
                throw new ProtobufException(
                        String.format(
                                "field '%s' cannot be found in protobuf schema (%s)",
                                fieldName, PROTOBUF_SCHEMA));
            }
            numbers[i] = fieldDescriptor.toProto().getNumber();
            javaTypes[i] = fieldDescriptor.getJavaType();
        }
        this.protoFieldNumbers = numbers;
        this.protoFieldJavaTypes = javaTypes;
    }
}
