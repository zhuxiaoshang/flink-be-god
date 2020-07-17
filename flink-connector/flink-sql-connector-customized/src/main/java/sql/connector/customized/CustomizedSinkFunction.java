package sql.connector.customized;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

public class CustomizedSinkFunction implements SinkFunction<Row> {
    private final String job;
    private final String metrics;
    private final String address;
    private final String[] fieldNames;
    public CustomizedSinkFunction(String job, String metrics, String address, String[] fieldNames) {
        this.job = job;
        this.metrics = metrics;
        this.address = address;
        this.fieldNames = fieldNames;
    }


    /**
     * Writes the given value to the sink. This function is called for every record.
     *
     * <p>You have to override this method when implementing a {@code SinkFunction}, this is a
     * {@code default} method for backward compatibility with the old-style method only.
     *
     * @param value   The input record.
     * @param context Additional context about the input record.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
     *                   to fail and may trigger recovery.
     */
    @Override
    public void invoke(Row value, Context context) throws Exception {
        /**
         * 这里是具体的数据发送逻辑，对接外部中间件
         */
    }
}
