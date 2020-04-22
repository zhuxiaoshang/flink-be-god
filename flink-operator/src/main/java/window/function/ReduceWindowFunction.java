package window.function;

/**
 * 基于window的reduce function，和普通reduce类似
 * fold function同理
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html#foldfunction
 */
public class ReduceWindowFunction {
    /**
     * DataStream<Tuple2<String, Long>> input = ...;
     *
     * input
     *     .keyBy(<key selector>)
     *     .window(<window assigner>)
     *     .reduce(new ReduceFunction<Tuple2<String, Long>> {
     *       public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) {
     *         return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
     *       }
     *     });
     */

    /**
     * DataStream<Tuple2<String, Long>> input = ...;
     *
     * input
     *     .keyBy(<key selector>)
     *     .window(<window assigner>)
     *     .fold("", new FoldFunction<Tuple2<String, Long>, String>> {
     *        public String fold(String acc, Tuple2<String, Long> value) {
     *          return acc + value.f1;
     *        }
     *     });
     */
}
