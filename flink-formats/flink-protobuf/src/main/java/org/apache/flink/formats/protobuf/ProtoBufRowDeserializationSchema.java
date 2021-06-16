package org.apache.flink.formats.protobuf;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.protobuf.messageutils.ProtobufRowSchemaConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;

public class ProtoBufRowDeserializationSchema implements DeserializationSchema<Row> {

    private static final long serialVersionUID = -228294630388809195L;

    /** Type information describing the result type. */
    private final RowTypeInfo typeInfo;

    /** Schema version describing the protobuf message file */
    private final String messageVersion;
    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final Boolean ignoreParseErrors;

    private final Boolean failOnMissingField;

    /** @ProtobufRowSchemaConverter working for deserialization. */
    private final ProtobufRowSchemaConverter converter;

    private ProtoBufRowDeserializationSchema(
            RowTypeInfo typeInfo, String messageVersion, boolean ignoreParseErrors) {
        this.typeInfo = typeInfo;
        this.messageVersion = messageVersion;
        this.ignoreParseErrors = ignoreParseErrors;
        this.failOnMissingField = false;
        this.converter =
                ProtobufRowSchemaConverter.builder()
                        .setRowTypeInfo(typeInfo)
                        .setIgnoreParseErrors(ignoreParseErrors)
                        .setMessageVersion(messageVersion)
                        .build();
    }

    public void deserialize(byte[] message, Collector<Row> out) throws IOException {
        Row row = deserialize(message);
        if (row != null) {
            out.collect(row);
        }
    }

    /**
     * Gets the data type (as a {@link TypeInformation}) produced by this function or input format.
     *
     * @return The data type produced by this function or input format.
     */
    @Override
    public TypeInformation<Row> getProducedType() {
        return typeInfo;
    }

    /**
     * Deserializes the byte message.
     *
     * @param message The message, as a byte array.
     * @return The deserialized message as an object (null if the message cannot be deserialized).
     */
    @Override
    public Row deserialize(byte[] message) throws IOException {
        try {
            Object[] objects = converter.concertProtobufMessageToObjects(message);
            return convertObjectsToRow(objects);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize Protobuf record.", e);
        }
    }

    /**
     * Method to decide whether the element signals the end of the stream. If true is returned the
     * element won't be emitted.
     *
     * @param nextElement The element to test for the end-of-stream signal.
     * @return True, if the element signals end of stream, false otherwise.
     */
    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    public Row convertObjectsToRow(Object[] objects) {
        TypeInformation<?>[] fieldTypes = typeInfo.getFieldTypes();
        int length = fieldTypes.length;
        Row row = new Row(length);
        for (int i = 0; i < length; i++) {
            row.setField(i, convertType(objects[i], fieldTypes[i]));
        }
        return row;
    }

    public Object convertType(Object object, TypeInformation<?> info) {
        //		if (object == null) {
        //			return object;
        //		}
        //		if (info == Types.STRING) {
        //			return object.toString();
        //		}
        return object;
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
        return Objects.hash(typeInfo, ignoreParseErrors, messageVersion);
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
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        final ProtoBufRowDeserializationSchema that = (ProtoBufRowDeserializationSchema) obj;
        return typeInfo.equals(that.typeInfo)
                && ignoreParseErrors == that.ignoreParseErrors
                && messageVersion.equals(that.messageVersion);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** A builder for creating a {@link ProtoBufRowDeserializationSchema} */
    @PublicEvolving
    public static class Builder {
        private RowTypeInfo typeInfo;
        private String messageVersion;
        private Boolean ignoreParseErrors = ProtoBufOptions.IGNORE_PARSE_ERRORS.defaultValue();

        public Builder() {}

        public Builder setTypeInfo(TypeInformation<Row> typeInfo) {
            Preconditions.checkNotNull(typeInfo, "Type information must not be null.");
            if (!(typeInfo instanceof RowTypeInfo)) {
                throw new IllegalArgumentException("Row type information expected.");
            }
            this.typeInfo = (RowTypeInfo) typeInfo;
            return this;
        }

        public Builder setMessageVersion(String messageVersion) {
            this.messageVersion = messageVersion;
            return this;
        }

        public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
            this.ignoreParseErrors = ignoreParseErrors;
            return this;
        }

        public ProtoBufRowDeserializationSchema build() {
            return new ProtoBufRowDeserializationSchema(
                    typeInfo, messageVersion, ignoreParseErrors);
        }
    }
}
