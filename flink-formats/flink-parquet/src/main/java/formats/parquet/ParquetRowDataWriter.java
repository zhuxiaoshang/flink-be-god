/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package formats.parquet;

import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.util.Preconditions;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;

import static org.apache.flink.formats.parquet.utils.ParquetSchemaConverter.computeMinBytesForDecimalPrecision;
import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.*;

/**
 * Writes a record to the Parquet API with the expected schema in order to be written to a file.
 */
public class ParquetRowDataWriter {

    private final RecordConsumer recordConsumer;
    private final boolean utcTimestamp;

    private final FieldWriter[] filedWriters;
    private final String[] fieldNames;

    public ParquetRowDataWriter(
            RecordConsumer recordConsumer,
            RowType rowType,
            GroupType schema,
            boolean utcTimestamp) {
        this.recordConsumer = recordConsumer;
        this.utcTimestamp = utcTimestamp;
        this.filedWriters = new FieldWriter[rowType.getFieldCount()];
        this.fieldNames = rowType.getFieldNames().toArray(new String[0]);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.filedWriters[i] = createWriter(rowType.getTypeAt(i), schema.getType(i));
        }
    }

    /**
     * It writes a record to Parquet.
     *
     * @param record Contains the record that is going to be written.
     */
    public void write(final RowData record) {
        recordConsumer.startMessage();
        for (int i = 0; i < filedWriters.length; i++) {
            if (!record.isNullAt(i)) {
                String fieldName = fieldNames[i];
                FieldWriter writer = filedWriters[i];

                recordConsumer.startField(fieldName, i);
                writer.write(record, i);
                recordConsumer.endField(fieldName, i);
            }
        }
        recordConsumer.endMessage();
    }

    private FieldWriter createWriter(LogicalType t, Type type) {
        if (type.isPrimitive()) {
            switch (t.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return new StringWriter();
                case BOOLEAN:
                    return new BooleanWriter();
                case BINARY:
                case VARBINARY:
                    return new BinaryWriter();
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) t;
                    return createDecimalWriter(decimalType.getPrecision(), decimalType.getScale());
                case TINYINT:
                    return new ByteWriter();
                case SMALLINT:
                    return new ShortWriter();
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                case INTEGER:
                    return new IntWriter();
                case BIGINT:
                    return new LongWriter();
                case FLOAT:
                    return new FloatWriter();
                case DOUBLE:
                    return new DoubleWriter();
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    TimestampType timestampType = (TimestampType) t;
                    return new TimestampWriter(timestampType.getPrecision());
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    LocalZonedTimestampType localZonedTimestampType = (LocalZonedTimestampType) t;
                    return new TimestampWriter(localZonedTimestampType.getPrecision());
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + type);
            }
        } else {
            switch (t.getTypeRoot()) {
                case MAP:
                    return new MapWriter((MapType) t, type);
                case ROW:
                    return new RowWriter((RowType) t, type);
                case ARRAY:
                    return new ArrayWriter((ArrayType) t, type);
                default:
                    throw new IllegalArgumentException("Unsupported  data type: " + t);
            }
        }
    }

    private interface FieldWriter {

        void write(RowData row, int ordinal);
    }

    private class MapWriter implements FieldWriter {
        private MapType mapType;
        private Type type;

        public MapWriter(MapType mapType, Type type) {
            this.mapType = mapType;
            this.type = type;
        }

        @Override
        public void write(RowData row, int ordinal) {
            final BinaryMapData map = (BinaryMapData) row.getMap(ordinal);
            final int size = map.size();
            final GroupType maptype = type.asGroupType().getType(0).asGroupType();
            final Type keyType = maptype.getType(0);
            final Type valueType = maptype.getType(1);
            final LogicalType logicalKeyType = mapType.getKeyType();
            final LogicalType logicalValueType = mapType.getValueType();

            recordConsumer.startGroup();
            if (size > 0) {
                Map<?, ?> javaMap = map.toJavaMap(logicalKeyType, logicalValueType);
                FieldWriter keyWriter = createWriter(logicalKeyType, keyType);
                FieldWriter valueWriter = createWriter(logicalValueType, valueType);
                recordConsumer.startField("key_value", 0);
                for (Map.Entry entry : javaMap.entrySet()) {
                    recordConsumer.startGroup();
                    recordConsumer.startField("key", 0);
                    keyWriter.write(GenericRowData.of(entry.getKey()), 0);
                    recordConsumer.endField("key", 0);
                    if (entry.getValue() != null) {
                        recordConsumer.startField("value", 1);
                        valueWriter.write(GenericRowData.of(entry.getValue()), 0);
                        recordConsumer.endField("value", 1);
                    }
                    recordConsumer.endGroup();
                }
                recordConsumer.endField("key_value", 0);
            }
            recordConsumer.endGroup();
        }
    }

    private class ArrayWriter implements FieldWriter {
        private ArrayType arrayType;
        private Type type;

        public ArrayWriter(ArrayType arrayType, Type type) {
            this.arrayType = arrayType;
            this.type = type;
        }

        @Override
        public void write(RowData row, int ordinal) {
            BinaryArrayData array = (BinaryArrayData) row.getArray(ordinal);
            LogicalType logicalType = arrayType.getElementType();
            Object[] objects = array.toObjectArray(logicalType);
            Type elementType = this.type.asGroupType().getType(0).asGroupType().getType(0);
            recordConsumer.startGroup(); // group wrapper (original type LIST)
            if (objects.length > 0) {
                recordConsumer.startField("list", 0);
                for (int i = 0; i < objects.length; i++) {
                    recordConsumer.startGroup();
                    recordConsumer.startField(elementType.getName(), 0);
                    FieldWriter writer = createWriter(logicalType, elementType);
                    writer.write(GenericRowData.of(objects[i]), 0);
                    recordConsumer.endField(elementType.getName(), 0);
                    recordConsumer.endGroup();
                }
                recordConsumer.endField("list", 0);
            }

            recordConsumer.endGroup();
        }
    }

    private class RowWriter implements FieldWriter {
        private RowType rowType;
        private Type type;

        public RowWriter(RowType rowType, Type type) {
            this.rowType = rowType;
            this.type = type;
        }

        @Override
        public void write(RowData rowData, int ordinal) {
            RowData row = rowData.getRow(ordinal, rowType.getFieldCount());
            final int size = row.getArity();
            final GroupType groupType = type.asGroupType();
            recordConsumer.startGroup();
            for (int i = 0; i < size; i++) {
                if (!row.isNullAt(i)) {
                    recordConsumer.startField(groupType.getFieldName(i), i);
                    FieldWriter writer = createWriter(rowType.getTypeAt(i), groupType.getType(i));
                    writer.write(row, i);
                    recordConsumer.endField(groupType.getFieldName(i), i);
                }
            }
            recordConsumer.endGroup();
        }
    }

    private class BooleanWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addBoolean(row.getBoolean(ordinal));
        }
    }

    private class ByteWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addInteger(row.getByte(ordinal));
        }
    }

    private class ShortWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addInteger(row.getShort(ordinal));
        }
    }

    private class LongWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addLong(row.getLong(ordinal));
        }
    }

    private class FloatWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addFloat(row.getFloat(ordinal));
        }
    }

    private class DoubleWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addDouble(row.getDouble(ordinal));
        }
    }

    private class StringWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addBinary(Binary.fromReusedByteArray(row.getString(ordinal).toBytes()));
        }
    }

    private class BinaryWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addBinary(Binary.fromReusedByteArray(row.getBinary(ordinal)));
        }
    }

    private class IntWriter implements FieldWriter {

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addInteger(row.getInt(ordinal));
        }
    }

    /**
     * We only support INT96 bytes now, julianDay(4) + nanosOfDay(8). See
     * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
     * TIMESTAMP_MILLIS and TIMESTAMP_MICROS are the deprecated ConvertedType.
     */
    private class TimestampWriter implements FieldWriter {

        private final int precision;

        private TimestampWriter(int precision) {
            this.precision = precision;
        }

        @Override
        public void write(RowData row, int ordinal) {
            recordConsumer.addBinary(timestampToInt96(row.getTimestamp(ordinal, precision)));
        }
    }

    private Binary timestampToInt96(TimestampData timestampData) {
        int julianDay;
        long nanosOfDay;
        if (utcTimestamp) {
            long mills = timestampData.getMillisecond();
            julianDay = (int) ((mills / MILLIS_IN_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
            nanosOfDay =
                    (mills % MILLIS_IN_DAY) * NANOS_PER_MILLISECOND
                            + timestampData.getNanoOfMillisecond();
        } else {
            Timestamp timestamp = timestampData.toTimestamp();
            long mills = timestamp.getTime();
            julianDay = (int) ((mills / MILLIS_IN_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
            nanosOfDay = ((mills % MILLIS_IN_DAY) / 1000) * NANOS_PER_SECOND + timestamp.getNanos();
        }

        ByteBuffer buf = ByteBuffer.allocate(12);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(nanosOfDay);
        buf.putInt(julianDay);
        buf.flip();
        return Binary.fromConstantByteBuffer(buf);
    }

    private FieldWriter createDecimalWriter(int precision, int scale) {
        Preconditions.checkArgument(
                precision <= DecimalType.MAX_PRECISION,
                "Decimal precision %s exceeds max precision %s",
                precision,
                DecimalType.MAX_PRECISION);

        /*
         * This is optimizer for UnscaledBytesWriter.
         */
        class LongUnscaledBytesWriter implements FieldWriter {
            private final int numBytes;
            private final int initShift;
            private final byte[] decimalBuffer;

            private LongUnscaledBytesWriter() {
                this.numBytes = computeMinBytesForDecimalPrecision(precision);
                this.initShift = 8 * (numBytes - 1);
                this.decimalBuffer = new byte[numBytes];
            }

            @Override
            public void write(RowData row, int ordinal) {
                long unscaledLong = row.getDecimal(ordinal, precision, scale).toUnscaledLong();
                int i = 0;
                int shift = initShift;
                while (i < numBytes) {
                    decimalBuffer[i] = (byte) (unscaledLong >> shift);
                    i += 1;
                    shift -= 8;
                }

                recordConsumer.addBinary(Binary.fromReusedByteArray(decimalBuffer, 0, numBytes));
            }
        }

        class UnscaledBytesWriter implements FieldWriter {
            private final int numBytes;
            private final byte[] decimalBuffer;

            private UnscaledBytesWriter() {
                this.numBytes = computeMinBytesForDecimalPrecision(precision);
                this.decimalBuffer = new byte[numBytes];
            }

            @Override
            public void write(RowData row, int ordinal) {
                byte[] bytes = row.getDecimal(ordinal, precision, scale).toUnscaledBytes();
                byte[] writtenBytes;
                if (bytes.length == numBytes) {
                    // Avoid copy.
                    writtenBytes = bytes;
                } else {
                    byte signByte = bytes[0] < 0 ? (byte) -1 : (byte) 0;
                    Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte);
                    System.arraycopy(
                            bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length);
                    writtenBytes = decimalBuffer;
                }
                recordConsumer.addBinary(Binary.fromReusedByteArray(writtenBytes, 0, numBytes));
            }
        }

        // 1 <= precision <= 18, writes as FIXED_LEN_BYTE_ARRAY
        // optimizer for UnscaledBytesWriter
        if (DecimalDataUtils.is32BitDecimal(precision)
                || DecimalDataUtils.is64BitDecimal(precision)) {
            return new LongUnscaledBytesWriter();
        }

        // 19 <= precision <= 38, writes as FIXED_LEN_BYTE_ARRAY
        return new UnscaledBytesWriter();
    }
}
