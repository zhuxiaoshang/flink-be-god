package sql.connector.customized;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;


public class CustomizedTableSink implements DynamicTableSink {
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
     * Returns the set of changes that the sink accepts during runtime.
     *
     * <p>The planner can make suggestions but the sink has the final decision what it requires. If
     * the planner does not support this mode, it will throw an error. For example, the sink can
     * return that it only supports {@link ChangelogMode#insertOnly()}.
     *
     * @param requestedMode expected set of changes by the current plan
     */
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    /**
     * Returns a provider of runtime implementation for writing the data.
     *
     * <p>There might exist different interfaces for runtime implementation which is why {@link SinkRuntimeProvider}
     * serves as the base interface. Concrete {@link SinkRuntimeProvider} interfaces might be located
     * in other Flink modules.
     *
     * <p>Independent of the provider interface, the table runtime expects that a sink implementation
     * accepts internal data structures (see {@link RowData} for more information).
     *
     * <p>The given {@link Context} offers utilities by the planner for creating runtime implementation
     * with minimal dependencies to internal data structures.
     *
     * <p>See {@code org.apache.flink.table.connector.sink.SinkFunctionProvider} in {@code flink-table-api-java-bridge}.
     *
     * @param context
     */
    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SinkFunction sinkFunction = new CustomizedSinkFunction(job, metrics, address, schema.getFieldNames());
        return SinkFunctionProvider.of(sinkFunction);
    }

    /**
     * Creates a copy of this instance during planning. The copy should be a deep copy of all mutable
     * members.
     */
    @Override
    public DynamicTableSink copy() {
        return new CustomizedTableSink(job,metrics,address,schema);
    }

    /**
     * Returns a string that summarizes this sink for printing to a console or log.
     */
    @Override
    public String asSummaryString() {
        return "custom table sink";
    }
}
