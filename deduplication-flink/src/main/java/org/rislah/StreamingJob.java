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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.curator4.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.curator4.com.google.common.hash.Funnels;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.rislah.common.PageView;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;

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
        KafkaSource<PageView> pageViewKafkaSource = KafkaSource.<PageView>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("dedupe")
                .setGroupId("dedupe-group")
                .setValueOnlyDeserializer(new DeserializationSchema<PageView>() {
                    @Override
                    public PageView deserialize(byte[] message) throws IOException {
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
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        DataStreamSource<PageView> pageViewStream = env.fromSource(
                pageViewKafkaSource,
                WatermarkStrategy.<PageView>forMonotonousTimestamps()
                        .withIdleness(Duration.ofSeconds(1)),
                "source");

        pageViewStream
                .process(new BloomDeduplicationProcessFunction())
//                .process(new MapDeduplicationProcessFunction())
                .print();


        env.execute("Flink Streaming Java API Skeleton");
    }

    public static class MapDeduplicationProcessFunction extends ProcessFunction<PageView, PageView> {
        private MapState<PageView, Void> seen;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            seen = getRuntimeContext().getMapState(new MapStateDescriptor<PageView, Void>(
                    "pageviews-seen",
                    TypeInformation.of(PageView.class), TypeInformation.of(Void.class)
            ));
        }

        @Override
        public void processElement(PageView pageView, ProcessFunction<PageView, PageView>.Context context, Collector<PageView> collector) throws Exception {
            if (!seen.contains(pageView)) {
                seen.put(pageView, null);
                collector.collect(pageView);
            }
        }
    }

    public static class BloomDeduplicationProcessFunction extends ProcessFunction<PageView, PageView> {
        private static final int BF_CARDINAL_THRESHOLD = 10;
        private static final double BF_FALSE_POSITIVE_RATE = 0.01;
        private volatile BloomFilter<String> userIdFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            long start = System.currentTimeMillis();
            userIdFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), BF_CARDINAL_THRESHOLD, BF_FALSE_POSITIVE_RATE);
            long end = System.currentTimeMillis();
            System.out.println("Created Guava BloomFilter: " + (start - end));
        }

        @Override
        public void processElement(PageView pageView, ProcessFunction<PageView, PageView>.Context context, Collector<PageView> collector) throws Exception {
            String userId = pageView.getUserId();
            if (!userIdFilter.mightContain(userId)) {
                userIdFilter.put(userId);
                collector.collect(pageView);
            }
        }

        @Override
        public void close() throws Exception {
            userIdFilter = null;
        }
    }
}
