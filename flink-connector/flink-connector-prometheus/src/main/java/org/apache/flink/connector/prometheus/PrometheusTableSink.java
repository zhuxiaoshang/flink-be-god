package org.apache.flink.connector.prometheus;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

public class PrometheusTableSink implements AppendStreamTableSink<Row> {
    private final String address;
    private final TableSchema schema;
    private final int parallelism;

    public PrometheusTableSink(String address, TableSchema schema, int parallelism) {
        this.address = Preconditions.checkNotNull(address, "address must not be null.");
        this.schema = TableSchemaUtils.checkOnlyPhysicalColumns(schema);
        this.parallelism = parallelism;
    }

    /** @deprecated */
    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return schema.getFieldTypes();
    }

    /** @deprecated */
    @Override
    public TypeInformation<Row> getOutputType() {
        return schema.toRowType();
    }

    /** @deprecated */
    @Override
    public String[] getFieldNames() {
        return schema.getFieldNames();
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        return dataStream
                .addSink(
                        new PrometheusSinkFunction(
                                address, getFieldNames(), schema.getFieldDataTypes()))
                .setParallelism(parallelism <= 0 ? dataStream.getParallelism() : parallelism)
                .name(TableConnectorUtils.generateRuntimeName(this.getClass(), getFieldNames()));
    }

    /**
     * @param fieldNames
     * @param fieldTypes
     * @deprecated
     */
    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        if (!Arrays.equals(getFieldNames(), fieldNames)
                || !Arrays.equals(getFieldTypes(), fieldTypes)) {
            throw new ValidationException(
                    "Reconfiguration with different fields is not allowed. "
                            + "Expected: "
                            + Arrays.toString(getFieldNames())
                            + " / "
                            + Arrays.toString(getFieldTypes())
                            + ". "
                            + "But was: "
                            + Arrays.toString(fieldNames)
                            + " / "
                            + Arrays.toString(fieldTypes));
        }
        return this;
    }
}
