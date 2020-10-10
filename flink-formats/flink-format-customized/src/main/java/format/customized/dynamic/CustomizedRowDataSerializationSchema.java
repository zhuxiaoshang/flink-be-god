package format.customized.dynamic;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;

/**
 * @author: zhushang
 * @create: 2020-10-10 14:26
 **/

public class CustomizedRowDataSerializationSchema implements SerializationSchema<RowData> {
    @Override
    public void open(InitializationContext context) throws Exception {

    }

    @Override
    public byte[] serialize(RowData rowData) {
        return new byte[0];
    }
}
