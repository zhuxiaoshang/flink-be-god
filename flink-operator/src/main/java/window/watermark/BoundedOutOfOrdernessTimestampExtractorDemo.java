package window.watermark;

/**
 * 提供固定延迟的水印分配器，只需设置延迟大小，并实现时间戳抽取方法
 * @see{@link window.sideoutput.SideOutputOperation}
 */
public class BoundedOutOfOrdernessTimestampExtractorDemo {
    /**
     * assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, Long>>(Time.seconds(0)) {
     *                     @Override
     *                     public long extractTimestamp(Tuple3<String, Integer, Long> element) {
     *                         return element.f2;
     *                     }
     *                 })
     */

    /**
     * assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(20)).withTimestampAssigner((e,t)->e.f2));
     */

}
