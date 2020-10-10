package format.customized.dynamic;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/**
 * @author: zhushang
 * @create: 2020-10-10 14:27
 **/

public class CustomizedRowDataDeserializationSchema implements DeserializationSchema<RowData> {
    @Override
    public void open(InitializationContext context) throws Exception {

    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }
}
