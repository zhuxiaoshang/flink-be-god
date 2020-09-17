package sql.connector.customized.dynamic;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;


/**
 * source 实现ScanTableSource 一般表示当做普通数据源
 * 实现LookupTableSource 表示当做维表关联
 *
 * @author: zhushang
 * @create: 2020-09-05 01:20
 **/

public class CustomizedDynamicTableSource implements ScanTableSource, LookupTableSource {
    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        return TableFunctionProvider.of(new CustomizedLookupFunction());
    }

    @Override
    public ChangelogMode getChangelogMode() {
       return null;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        SourceFunction sourceFunction = new CustomizedSourceFunction();
        return SourceFunctionProvider.of(sourceFunction,false);
    }

    @Override
    public DynamicTableSource copy() {
        return new CustomizedDynamicTableSource();
    }

    @Override
    public String asSummaryString() {
        return "customized table source";
    }
}
