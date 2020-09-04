package sql.connector.customized.dynamic;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import sql.connector.customized.CustomizedSinkFunction;

/**
 * @author: zhushang
 * @create: 2020-09-05 01:19
 **/

public class CustomizedDynamicTableSink implements DynamicTableSink {
    private final TableSchema schema;

    public CustomizedDynamicTableSink(TableSchema schema) {
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE).build();//支持的sink类型，append、upsert、retract
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SinkFunction sinkFunction = new CustomizedSinkFunction("", "", "", schema.getFieldNames());
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new CustomizedDynamicTableSink(schema);
    }

    @Override
    public String asSummaryString() {
        return "customized table sink";
    }
}
