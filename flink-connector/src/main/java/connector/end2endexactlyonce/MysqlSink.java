package connector.end2endexactlyonce;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public class MysqlSink extends TwoPhaseCommitSinkFunction<List<String>, Connection, Void> {
    /**
     * Use default {@link ListStateDescriptor} for internal state serialization. Helpful utilities for using this
     * constructor are {@link TypeInformation#of(Class)}, {@link TypeHint} and
     * {@link TypeInformation#of(TypeHint)}. Example:
     * <pre>
     * {@code
     * TwoPhaseCommitSinkFunction(TypeInformation.of(new TypeHint<State<TXN, CONTEXT>>() {}));
     * }
     * </pre>
     *
     * @param transactionSerializer {@link TypeSerializer} for the transaction type of this sink
     * @param contextSerializer     {@link TypeSerializer} for the context type of this sink
     */
    public MysqlSink(TypeSerializer<Connection> transactionSerializer, TypeSerializer<Void> contextSerializer) {
        super(transactionSerializer, contextSerializer);
    }

    public MysqlSink() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * Write value within a transaction.
     *
     * @param transaction
     * @param values
     * @param context
     */
    @Override
    protected void invoke(Connection transaction, List<String> values, Context context) throws Exception {
        String sql = "insert into `mysql_test` (`user_id`,`item_id`,`category_id`,`behavior`,`ts`) values (?,?,?,?,?)";
        PreparedStatement ps = transaction.prepareStatement(sql);
        for (String val :
                values) {
            JSONObject jo = JSONObject.parseObject(val);
            ps.setLong(1, jo.getLong("user_id"));
            ps.setLong(2, jo.getLong("item_id"));
            ps.setLong(3, jo.getLong("category_id"));
            ps.setString(4, jo.getString("behavior"));
            ps.setTimestamp(5, jo.getTimestamp("ts"));
            ps.addBatch();
        }
        ps.executeBatch();
    }

    /**
     * Method that starts a new transaction.
     *
     * @return newly created transaction.
     */
    @Override
    protected Connection beginTransaction() throws Exception {
        String url = "jdbc:mysql://localhost:3306/zhushang?useUnicode=true&characterEncoding=UTF-8" +
                "&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true&keepAlive=true";
        Connection connection = DBConnectUtil.getConnection(url, "root", "123456");
        return connection;
    }

    /**
     * Pre commit previously created transaction. Pre commit must make all of the necessary steps to prepare the
     * transaction for a commit that might happen in the future. After this point the transaction might still be
     * aborted, but underlying implementation must ensure that commit calls on already pre committed transactions
     * will always succeed.
     *
     * <p>Usually implementation involves flushing the data.
     *
     * @param transaction
     */
    @Override
    protected void preCommit(Connection transaction) throws Exception {

    }

    /**
     * Commit a pre-committed transaction. If this method fail, Flink application will be
     * restarted and {@link TwoPhaseCommitSinkFunction#recoverAndCommit(Object)} will be called again for the
     * same transaction.
     *
     * @param transaction
     */
    @Override
    protected void commit(Connection transaction) {
        //提交事务
        DBConnectUtil.commit(transaction);
    }

    /**
     * Abort a transaction.
     *
     * @param transaction
     */
    @Override
    protected void abort(Connection transaction) {
        //回滚事务
        DBConnectUtil.rollback(transaction);
    }
}
