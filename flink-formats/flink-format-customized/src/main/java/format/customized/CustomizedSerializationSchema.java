package format.customized;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;

/**
 * @author: zhushang
 * @create: 2020-10-05 16:25
 **/

public class CustomizedSerializationSchema implements SerializationSchema<Row> {
    /**
     * Initialization method for the schema. It is called before the actual working methods
     * {@link #serialize(Object)} and thus suitable for one time setup work.
     *
     * <p>The provided {@link InitializationContext} can be used to access additional features such as e.g.
     * registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    @Override
    public void open(InitializationContext context) throws Exception {

    }

    /**
     * Serializes the incoming element to a specified type.
     *
     * @param element The incoming element to be serialized
     * @return The serialized element.
     */
    @Override
    public byte[] serialize(Row element) {
        return new byte[0];
    }
}
