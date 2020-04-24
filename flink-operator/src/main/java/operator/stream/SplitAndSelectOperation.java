package operator.stream;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * split/select算子，split可以把一个流分成多个流，通过select选择具体哪个流，已经废弃
 * DataStream → SplitStream → DataStream
 */
public class SplitAndSelectOperation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple3<String, Integer, Long>> src1 = env.fromElements(Tuple3.of("k1", 11, 1587314572000L), Tuple3.of("k2", 2, 1587314574000L)
                , Tuple3.of("k3", 3, 1587314576000L), Tuple3.of("k2", 5, 1587314575000L)
                , Tuple3.of("k3", 10, 1587314577000L), Tuple3.of("k1", 9, 1587314579000L));
        SplitStream<Tuple3<String, Integer, Long>> split = src1.split(new OutputSelector<Tuple3<String, Integer, Long>>() {
            @Override
            public Iterable<String> select(Tuple3<String, Integer, Long> value) {
                List<String> list = new ArrayList<>();
                if (value.f1 >= 10) {
                    list.add("bigthen10");
                } else {
                    list.add("lessthen10");
                }
                return list;
            }
        });
        split.select("bigthen10").print();

        //split.select("lessthen10").print();

        env.execute();

    }
}
