package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.protobuf.messageutils.ProtobufRowSchemaConverter;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ProtoBufRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /** Type information describing the result type. */
    private final TypeInformation<RowData> resultTypeInfo;

    /** Schema version describing the protobuf message file */
    private final String messageVersion;
    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final Boolean ignoreParseErrors;

    private final RowTypeInfo rowTypeInfo;

    private final RowType rowType;

    /** @ProtobufRowSchemaConverter working for deserialization. */
    private final ProtobufRowSchemaConverter converter;

    private final DeserializationRuntimeConverter runtimeConverter;

    public ProtoBufRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            String messageVersion,
            boolean ignoreParseErrors) {
        this.rowType = rowType;
        this.rowTypeInfo = createRowTypeInfo(resultTypeInfo);
        this.resultTypeInfo = resultTypeInfo;
        this.messageVersion = messageVersion;
        this.ignoreParseErrors = ignoreParseErrors;
        this.converter =
                ProtobufRowSchemaConverter.builder()
                        .setRowTypeInfo(rowTypeInfo)
                        .setIgnoreParseErrors(ignoreParseErrors)
                        .setMessageVersion(messageVersion)
                        .build();
        this.runtimeConverter = createRowDataConverter(rowType);
    }

    /**
     * Gets the data type (as a {@link TypeInformation}) produced by this function or input format.
     *
     * @return The data type produced by this function or input format.
     */
    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    /**
     * Deserializes the byte message.
     *
     * @param message The message, as a byte array.
     * @return The deserialized message as an object (null if the message cannot be deserialized).
     */
    @Override
    public RowData deserialize(byte[] message) throws IOException {
        Object[] objects = converter.concertProtobufMessageToObjects(message);
        if (objects != null) {
            return (RowData) runtimeConverter.convert(objects);
        }
        return null;
    }

    /**
     * Deserializes the byte message.
     *
     * <p>
     *
     * <p>Can output multiple records through the {@link Collector}. Note that number and size of
     * the produced records should be relatively small. Depending on the source implementation
     * records can be buffered in memory or collecting records might delay emitting checkpoint
     * barrier.
     *
     * @param message The message, as a byte array.
     * @param out The collector to put the resulting messages.
     */
    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        RowData rowData = deserialize(message);
        if (rowData != null) {
            out.collect(rowData);
        }
    }

    public RowData convertObjectsToRowData(Object[] objects) {
        TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
        int length = fieldTypes.length;
        GenericRowData rowData = new GenericRowData(length);
        for (int i = 0; i < length; i++) {
            rowData.setField(i, convertType(objects[i], fieldTypes[i]));
        }
        return rowData;
    }

    // TODO: 2020/8/19 Type conversion.
    public Object convertType(Object object, TypeInformation<?> info) {
        if (object == null) {
            return object;
        }
        if (info == Types.STRING) {
            return object.toString();
        }
        return object;
    }

    /**
     * Method to decide whether the element signals the end of the stream. If true is returned the
     * element won't be emitted.
     *
     * @param nextElement The element to test for the end-of-stream signal.
     * @return True, if the element signals end of stream, false otherwise.
     */
    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    private static RowTypeInfo createRowTypeInfo(TypeInformation<RowData> rowDataTypeInformation) {
        RowDataTypeInfo rowDataTypeInfo = (RowDataTypeInfo) rowDataTypeInformation;
        String[] fieldNames = rowDataTypeInfo.getFieldNames();
        TypeInformation<?>[] fieldTypes = rowDataTypeInfo.getFieldTypes();
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        return rowTypeInfo;
    }

    /**
     * Returns a hash code value for the object. This method is supported for the benefit of hash
     * tables such as those provided by {@link HashMap}.
     *
     * <p>The general contract of {@code hashCode} is:
     *
     * <ul>
     *   <li>Whenever it is invoked on the same object more than once during an execution of a Java
     *       application, the {@code hashCode} method must consistently return the same integer,
     *       provided no information used in {@code equals} comparisons on the object is modified.
     *       This integer need not remain consistent from one execution of an application to another
     *       execution of the same application.
     *   <li>If two objects are equal according to the {@code equals(Object)} method, then calling
     *       the {@code hashCode} method on each of the two objects must produce the same integer
     *       result.
     *   <li>It is <em>not</em> required that if two objects are unequal according to the {@link
     *       Object#equals(Object)} method, then calling the {@code hashCode} method on each of the
     *       two objects must produce distinct integer results. However, the programmer should be
     *       aware that producing distinct integer results for unequal objects may improve the
     *       performance of hash tables.
     * </ul>
     *
     * <p>As much as is reasonably practical, the hashCode method defined by class {@code Object}
     * does return distinct integers for distinct objects. (This is typically implemented by
     * converting the internal address of the object into an integer, but this implementation
     * technique is not required by the Java&trade; programming language.)
     *
     * @return a hash code value for this object.
     * @see Object#equals(Object)
     * @see System#identityHashCode
     */
    @Override
    public int hashCode() {
        return Objects.hash(ignoreParseErrors, resultTypeInfo, messageVersion);
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     *
     * <p>The {@code equals} method implements an equivalence relation on non-null object
     * references:
     *
     * <ul>
     *   <li>It is <i>reflexive</i>: for any non-null reference value {@code x}, {@code x.equals(x)}
     *       should return {@code true}.
     *   <li>It is <i>symmetric</i>: for any non-null reference values {@code x} and {@code y},
     *       {@code x.equals(y)} should return {@code true} if and only if {@code y.equals(x)}
     *       returns {@code true}.
     *   <li>It is <i>transitive</i>: for any non-null reference values {@code x}, {@code y}, and
     *       {@code z}, if {@code x.equals(y)} returns {@code true} and {@code y.equals(z)} returns
     *       {@code true}, then {@code x.equals(z)} should return {@code true}.
     *   <li>It is <i>consistent</i>: for any non-null reference values {@code x} and {@code y},
     *       multiple invocations of {@code x.equals(y)} consistently return {@code true} or
     *       consistently return {@code false}, provided no information used in {@code equals}
     *       comparisons on the objects is modified.
     *   <li>For any non-null reference value {@code x}, {@code x.equals(null)} should return {@code
     *       false}.
     * </ul>
     *
     * <p>The {@code equals} method for class {@code Object} implements the most discriminating
     * possible equivalence relation on objects; that is, for any non-null reference values {@code
     * x} and {@code y}, this method returns {@code true} if and only if {@code x} and {@code y}
     * refer to the same object ({@code x == y} has the value {@code true}).
     *
     * <p>Note that it is generally necessary to override the {@code hashCode} method whenever this
     * method is overridden, so as to maintain the general contract for the {@code hashCode} method,
     * which states that equal objects must have equal hash codes.
     *
     * @param obj the reference object with which to compare.
     * @return {@code true} if this object is the same as the obj argument; {@code false} otherwise.
     * @see #hashCode()
     * @see HashMap
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ProtoBufRowDataDeserializationSchema that = (ProtoBufRowDataDeserializationSchema) obj;
        return messageVersion.equals(that.messageVersion)
                && resultTypeInfo.equals(that.resultTypeInfo)
                && ignoreParseErrors.equals(that.ignoreParseErrors);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    /**
     * Runtime converter that converts ProtoBuf data structures into objects of Flink Table & SQL
     * internal data structures.
     */
    @FunctionalInterface
    interface DeserializationRuntimeConverter extends Serializable {
        Object convert(Object object);
    }

    static DeserializationRuntimeConverter createRowDataConverter(RowType rowType) {
        final int arity = rowType.getFieldCount();
        DeserializationRuntimeConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(ProtoBufRowDataDeserializationSchema::createNullableConverter)
                        .toArray(DeserializationRuntimeConverter[]::new);
        return objs -> {
            Object[] objects = (Object[]) objs;
            GenericRowData rowData = new GenericRowData(arity);
            for (int i = 0; i < arity; i++) {
                rowData.setField(i, fieldConverters[i].convert(objects[i]));
            }
            return rowData;
        };
    }

    private static DeserializationRuntimeConverter createNullableConverter(LogicalType type) {
        final DeserializationRuntimeConverter converter = createConverter(type);
        return object -> {
            if (object == null) {
                return null;
            }
            return converter.convert(object);
        };
    }

    private static DeserializationRuntimeConverter createConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return object -> null;
            case TINYINT:
                return object -> ((Integer) object).byteValue();
            case SMALLINT:
                return object -> ((Integer) object).shortValue();
            case BOOLEAN:
            case INTEGER:
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return object -> object;
            case CHAR:
            case VARCHAR:
                return object -> object == null ? null : StringData.fromString(object.toString());
            case ROW:
                return createRowConverter((RowType) type);
            case MAP:
                return createMapConverter(type);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static DeserializationRuntimeConverter createRowConverter(RowType type) {
        final LogicalType[] fieldTypes =
                type.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final DeserializationRuntimeConverter[] fieldConverters =
                Arrays.stream(fieldTypes)
                        .map(ProtoBufRowDataDeserializationSchema::createConverter)
                        .toArray(DeserializationRuntimeConverter[]::new);
        final int fieldCount = type.getFieldCount();

        return object -> {
            Row row = (Row) object;
            GenericRowData rowData = new GenericRowData(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                rowData.setField(i, fieldConverters[i].convert(row.getField(i)));
            }
            return rowData;
        };
    }

    private static DeserializationRuntimeConverter createMapConverter(LogicalType type) {
        MapType mapType = (MapType) type;
        LogicalType keyType = mapType.getKeyType();
        LogicalType valueType = mapType.getValueType();
        final DeserializationRuntimeConverter keyConverter = createConverter(keyType);
        final DeserializationRuntimeConverter valueConverter = createConverter(valueType);
        return object -> {
            final Map<?, ?> map = (Map<?, ?>) object;
            Map<Object, Object> result = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = keyConverter.convert(entry.getKey());
                Object value = valueConverter.convert(entry.getValue());
                result.put(key, value);
            }
            return new GenericMapData(result);
        };
    }
}
