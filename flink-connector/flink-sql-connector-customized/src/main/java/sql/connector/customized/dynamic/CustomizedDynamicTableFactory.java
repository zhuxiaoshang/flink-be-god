package sql.connector.customized.dynamic;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author: zhushang
 * @create: 2020-09-05 01:18
 **/

public class CustomizedDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {
    private static final ConfigOption<String> OPTION = ConfigOptions.key("option")
            .stringType()
            .defaultValue("default")
            .withDescription("description");
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return new CustomizedDynamicTableSink(context.getCatalogTable().getSchema());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return new CustomizedDynamicTableSource();
    }

    /**
     * 唯一标识符，用来让框架通过SPI来找到该connector
     * 1.11的配置为
     * 'connector' = 'customized'
     * @return
     */
    @Override
    public String factoryIdentifier() {
        return "customized";
    }

    /**
     * 必填参数
     * @return
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OPTION);
        return options;
    }

    /**
     * 可选参数
     * @return
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
