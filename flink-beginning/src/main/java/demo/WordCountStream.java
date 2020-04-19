package demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

public class WordCountStream {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(1000);
        env.setParallelism(1);
        String input = "Licensed to the Apache Software Foundation (ASF) under one or more\n" +
                " * contributor license agreements.  See the NOTICE file distributed with\n" +
                " * this work for additional information regarding copyright ownership.\n" +
                " * The ASF licenses this file to You under the Apache License, Version 2.0\n" +
                " * (the \"License\"); you may not use this file except in compliance with\n" +
                " * the License.  You may obtain a copy of the License at";
        env.fromElements(input).flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String s : value.split(" ")) {
                    collector.collect(Tuple2.of(s, 1));
                }
            }
        }).keyBy(0).sum(1).print();
        env.execute();
    }
}
