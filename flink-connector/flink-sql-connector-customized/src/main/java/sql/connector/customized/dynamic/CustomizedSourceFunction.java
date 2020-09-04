package sql.connector.customized.dynamic;

import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author: zhushang
 * @create: 2020-09-05 01:47
 **/

public class CustomizedSourceFunction implements SourceFunction {
    /**
     * Starts the source. Implementations can use the {@link SourceContext} emit
     * elements.
     *
     * <p>Sources that implement {@link CheckpointedFunction}
     * must lock on the checkpoint lock (using a synchronized block) before updating internal
     * state and emitting elements, to make both an atomic operation:
     *
     * <pre>{@code
     *  public class ExampleCountSource implements SourceFunction<Long>, CheckpointedFunction {
     *      private long count = 0L;
     *      private volatile boolean isRunning = true;
     *
     *      private transient ListState<Long> checkpointedCount;
     *
     *      public void run(SourceContext<T> ctx) {
     *          while (isRunning && count < 1000) {
     *              // this synchronized block ensures that state checkpointing,
     *              // internal state updates and emission of elements are an atomic operation
     *              synchronized (ctx.getCheckpointLock()) {
     *                  ctx.collect(count);
     *                  count++;
     *              }
     *          }
     *      }
     *
     *      public void cancel() {
     *          isRunning = false;
     *      }
     *
     *      public void initializeState(FunctionInitializationContext context) {
     *          this.checkpointedCount = context
     *              .getOperatorStateStore()
     *              .getListState(new ListStateDescriptor<>("count", Long.class));
     *
     *          if (context.isRestored()) {
     *              for (Long count : this.checkpointedCount.get()) {
     *                  this.count = count;
     *              }
     *          }
     *      }
     *
     *      public void snapshotState(FunctionSnapshotContext context) {
     *          this.checkpointedCount.clear();
     *          this.checkpointedCount.add(count);
     *      }
     * }
     * }</pre>
     *
     * @param ctx The context to emit elements to and for accessing locks.
     */
    @Override
    public void run(SourceContext ctx) throws Exception {

    }

    /**
     * Cancels the source. Most sources will have a while loop inside the
     * {@link #run(SourceContext)} method. The implementation needs to ensure that the
     * source will break out of that loop after this method is called.
     *
     * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
     * {@code false} in this method. That flag is checked in the loop condition.
     *
     * <p>When a source is canceled, the executing thread will also be interrupted
     * (via {@link Thread#interrupt()}). The interruption happens strictly after this
     * method has been called, so any interruption handler can rely on the fact that
     * this method has completed. It is good practice to make any flags altered by
     * this method "volatile", in order to guarantee the visibility of the effects of
     * this method to any interruption handler.
     */
    @Override
    public void cancel() {

    }
}
