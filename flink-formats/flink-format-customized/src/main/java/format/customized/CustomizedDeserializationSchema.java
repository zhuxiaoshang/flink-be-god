package format.customized;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @author: zhushang
 * @create: 2020-10-05 16:24
 **/

public class CustomizedDeserializationSchema implements DeserializationSchema<Row> {
    /**
     * Initialization method for the schema. It is called before the actual working methods
     * {@link #deserialize} and thus suitable for one time setup work.
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
     * Deserializes the byte message.
     *
     * @param message The message, as a byte array.
     * @return The deserialized message as an object (null if the message cannot be deserialized).
     */
    @Override
    public Row deserialize(byte[] message) throws IOException {
        return null;
    }

    /**
     * Deserializes the byte message.
     *
     * <p>Can output multiple records through the {@link Collector}. Note that number and size of the
     * produced records should be relatively small. Depending on the source implementation records
     * can be buffered in memory or collecting records might delay emitting checkpoint barrier.
     *
     * @param message The message, as a byte array.
     * @param out     The collector to put the resulting messages.
     */
    @Override
    public void deserialize(byte[] message, Collector<Row> out) throws IOException {

    }

    /**
     * Method to decide whether the element signals the end of the stream. If
     * true is returned the element won't be emitted.
     *
     * @param nextElement The element to test for the end-of-stream signal.
     * @return True, if the element signals end of stream, false otherwise.
     */
    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    /**
     * Gets the data type (as a {@link TypeInformation}) produced by this function or input format.
     *
     * @return The data type produced by this function or input format.
     */
    @Override
    public TypeInformation<Row> getProducedType() {
        return null;
    }
}
