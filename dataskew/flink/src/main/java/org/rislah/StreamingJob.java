/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.rislah;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.rislah.common.PageView;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        final ObjectMapper objectMapper = new ObjectMapper();

        env.setParallelism(10);

        KafkaSource<PageView> pageViewKafkaSource = KafkaSource.<PageView>builder()
                .setBootstrapServers("localhost:9094")
                .setTopics("page.view")
                .setGroupId("dataskew-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DeserializationSchema<PageView>() {
                    @Override
                    public PageView deserialize(byte[] message) throws IOException {
                        if (message == null)
                            return null;
                        return objectMapper.readValue(message, PageView.class);
                    }

                    @Override
                    public boolean isEndOfStream(PageView nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<PageView> getProducedType() {
                        return TypeInformation.of(PageView.class);
                    }
                })
                .build();

        DataStreamSource<PageView> pageViewStream = env.fromSource(
                pageViewKafkaSource,
                WatermarkStrategy.<PageView>forMonotonousTimestamps()
                        .withIdleness(Duration.ofSeconds(1)),
                "source"
        );

        pageViewStream
                .map(p -> {
                    String platform = p.getPlatform();
                    p.setPlatform(platform + "@" + ThreadLocalRandom.current().nextInt(20));
                    return p;
                })
                .keyBy(PageView::getPlatform)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .trigger(new Trigger<PageView, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(PageView element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        System.out.println("processing time trigger");
                        return TriggerResult.FIRE_AND_PURGE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.FIRE_AND_PURGE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                    }
                })
                .aggregate(new AggregateFunction<PageView, PageViewAccumulator, PageViewAccumulator>() {
                               @Override
                               public PageViewAccumulator createAccumulator() {
                                   return new PageViewAccumulator();
                               }

                               @Override
                               public PageViewAccumulator add(PageView value, PageViewAccumulator accumulator) {
                                   if (accumulator.getKey().isEmpty()) {
                                       accumulator.setKey(value.getPlatform());
                                   }
                                   accumulator.addCount(1);
                                   accumulator.addUserId(value.getUserId());
                                   return accumulator;
                               }

                               @Override
                               public PageViewAccumulator getResult(PageViewAccumulator accumulator) {
                                   return accumulator;
                               }

                               @Override
                               public PageViewAccumulator merge(PageViewAccumulator a, PageViewAccumulator b) {
                                   if (a.getKey().isEmpty()) {
                                       a.setKey(b.getKey());
                                   }
                                   a.addCount(b.getCount());
                                   a.addUserIDs(b.getUserIds());
                                   return a;
                               }
                           },
                        new WindowFunction<PageViewAccumulator, WindowedViewSum, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<PageViewAccumulator> input, Collector<WindowedViewSum> out) throws Exception {
                                PageViewAccumulator acc = input.iterator().next();
                                String key = acc.getKey();
                                out.collect(new WindowedViewSum(
                                                key.substring(0, key.indexOf("@")),
                                                window.getStart(),
                                                window.getEnd(),
                                                acc.getCount(),
                                                acc.getUserIds()
                                        )
                                );
                            }
                        })
                .keyBy(WindowedViewSum::getWindowEndTimestamp)
                .process(new KeyedProcessFunction<Long, WindowedViewSum, Tuple3<String, Integer, Set<String>>>() {
                    @Override
                    public void processElement(WindowedViewSum value, KeyedProcessFunction<Long, WindowedViewSum, Tuple3<String, Integer, Set<String>>>.Context ctx, Collector<Tuple3<String, Integer, Set<String>>> out) throws Exception {
                        Tuple3<String, Integer, Set<String>> sum = new Tuple3<>(value.getKey(), value.getCount(), new HashSet<>(2048));
                        sum.f2.addAll(value.getUserIds());
                        out.collect(sum);
                    }
                })
                .print()
                .setParallelism(1);

        env.execute("Flink Streaming Java API Skeleton");
    }

    @Data
    @AllArgsConstructor
    @EqualsAndHashCode
    private static class WindowedViewSum {
        private String key;
        private Long windowStartTimestamp;
        private Long windowEndTimestamp;
        private int count;
        private Set<String> userIds;
    }

    public static class PageViewAccumulator extends Tuple3<String, Integer, Set<String>> {
        public PageViewAccumulator() {
            super("", 0, new HashSet<>(2048));
        }

        public PageViewAccumulator(String f0, Integer f1, Set<String> f2) {
            super(f0, f1, f2);
        }

        @Override
        public String toString() {
            return "PageViewAccumulator{" +
                    "f0=" + f0 +
                    ", f1=" + f1 +
                    ", f2=" + f2 +
                    '}';
        }

        public String getKey() {
            return this.f0;
        }

        public void setKey(String key) {
            this.f0 = key;
        }

        public void addCount(int count) {
            setCount(this.f1 + count);
        }

        public int getCount() {
            return this.f1;
        }

        public void setCount(int count) {
            this.f1 = count;
        }

        public Set<String> getUserIds() {
            return this.f2;
        }

        public void addUserId(String userId) {
            this.f2.add(userId);
        }

        public void addUserIDs(Set<String> userIds) {
            this.f2.addAll(userIds);
        }
    }
}
