package sql.connector.customized;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

public class CustomizedTableSink implements AppendStreamTableSink<Row> {
    private final String job;
    private final String metrics;
    private final String address;
    private final TableSchema schema;
    public CustomizedTableSink(String job, String metrics, String address, TableSchema schema) {
        this.job = Preconditions.checkNotNull(job, "job must not be null.");
        this.metrics = Preconditions.checkNotNull(metrics, "metrics must not be null.");
        this.address = Preconditions.checkNotNull(address, "address must not be null.");
        this.schema = TableSchemaUtils.checkNoGeneratedColumns(schema);
    }
    /**
     * @deprecated
     */
    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return schema.getFieldTypes();
    }

    /**
     * @deprecated
     */
    @Override
    public TypeInformation<Row> getOutputType() {
        return schema.toRowType();
    }


    /**
     * @deprecated
     */
    @Override
    public String[] getFieldNames() {
        return schema.getFieldNames();
    }

    /**
     * Emits the DataStream.
     *
     * @param dataStream
     * @deprecated This method will be removed in future versions as it returns nothing.
     * It is recommended to use {@link #consumeDataStream(DataStream)} instead which
     * returns the {@link DataStreamSink}. The returned {@link DataStreamSink} will be
     * used to set resources for the sink operator. If the {@link #consumeDataStream(DataStream)}
     * is implemented, this method can be empty implementation.
     */
    @Override
    public void emitDataStream(DataStream<Row> dataStream) {

    }

    /**
     * Consumes the DataStream and return the sink transformation {@link DataStreamSink}.
     * The returned {@link DataStreamSink} will be used to set resources for the sink operator.
     *
     * @param dataStream
     */
    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        return dataStream
                .addSink(new CustomizedSinkFunction(job, metrics, address, getFieldNames()))
                .setParallelism(dataStream.getParallelism())
                .name(TableConnectorUtils.generateRuntimeName(this.getClass(), getFieldNames()));
    }

    /**
     * Returns a copy of this {@link TableSink} configured with the field names and types of the
     * table to emit.
     *
     * @param fieldNames The field names of the table to emit.
     * @param fieldTypes The field types of the table to emit.
     * @return A copy of this {@link TableSink} configured with the field names and types of the
     * table to emit.
     * @deprecated This method will be dropped in future versions. It is recommended to pass a
     * static schema when instantiating the sink instead.
     */
    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
            throw new ValidationException("Reconfiguration with different fields is not allowed. " +
                    "Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
                    "But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
        }
        return this;
    }
}
