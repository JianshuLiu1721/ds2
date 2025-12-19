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

package ch.ethz.systems.strymon.ds2.flink.nexmark.queries;

import ch.ethz.systems.strymon.ds2.flink.nexmark.sinks.DummyLatencyCountingSink;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.BidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class Query5v2 {

    private static final Logger logger  = LoggerFactory.getLogger(Query5v2.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        final int srcRate = params.getInt("srcRate", 100000);

        // Step 1: Source with timestamp tracking
        DataStream<BidWithTimestamp> bidsWithTimestamp = env.addSource(new BidSourceFunction(srcRate))
                .setParallelism(params.getInt("p-bid-source", 1))
                .map(new MapFunction<Bid, BidWithTimestamp>() {
                    @Override
                    public BidWithTimestamp map(Bid bid) throws Exception {
                        BidWithTimestamp bwt = new BidWithTimestamp();
                        bwt.bid = bid;
                        bwt.sourceEmitTime = System.currentTimeMillis();
                        return bwt;
                    }
                })
                .name("Add Source Timestamp");

        // Step 2: Assign timestamps and watermarks
        DataStream<BidWithTimestamp> timestamped = bidsWithTimestamp
                .assignTimestampsAndWatermarks(new TimestampAssigner())
                .map(new MapFunction<BidWithTimestamp, BidWithTimestamp>() {
                    @Override
                    public BidWithTimestamp map(BidWithTimestamp bwt) throws Exception {
                        bwt.afterTimestampTime = System.currentTimeMillis();
                        return bwt;
                    }
                })
                .name("Add Timestamp Assignment Time");

        // Step 3: Before window - add timestamp
        DataStream<BidWithTimestamp> beforeWindow = timestamped
                .map(new MapFunction<BidWithTimestamp, BidWithTimestamp>() {
                    @Override
                    public BidWithTimestamp map(BidWithTimestamp bwt) throws Exception {
                        bwt.beforeWindowTime = System.currentTimeMillis();
                        return bwt;
                    }
                })
                .name("Add Before Window Time");

        // Step 4: Window and aggregate
        // SELECT B1.auction, count(*) AS num
        // FROM Bid [RANGE 10 SECOND SLIDE 1 SECOND] B1
        // GROUP BY B1.auction
        DataStream<LatencyTrackedTuple> windowed = beforeWindow
                .keyBy(new KeySelector<BidWithTimestamp, Long>() {
                    @Override
                    public Long getKey(BidWithTimestamp bwt) throws Exception {
                        return bwt.bid.auction;
                    }
                })
                .timeWindow(Time.seconds(10), Time.seconds(1))
                .aggregate(new CountBidsWithLatency())
                .name("Sliding Window")
                .setParallelism(params.getInt("p-window", 1));

        // Step 5: After window - add timestamp
        DataStream<LatencyTrackedTuple> afterWindow = windowed
                .map(new MapFunction<LatencyTrackedTuple, LatencyTrackedTuple>() {
                    @Override
                    public LatencyTrackedTuple map(LatencyTrackedTuple ltt) throws Exception {
                        ltt.setAfterWindowTime(System.currentTimeMillis());
                        return ltt;
                    }
                })
                .name("Add After Window Time");

        // Step 6: Sink (will add sink receive time)
        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        afterWindow.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-window", 1));
        env.execute("Nexmark Query5v2");
    }

    /**
     * Wrapper class for Bid with source timestamp
     */
    private static class BidWithTimestamp {
        public Bid bid;
        public long sourceEmitTime;
        public long afterTimestampTime;
        public long beforeWindowTime;
    }

    /**
     * Timestamp assigner for BidWithTimestamp
     */
    private static final class TimestampAssigner implements AssignerWithPeriodicWatermarks<BidWithTimestamp> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(BidWithTimestamp element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.bid.dateTime);
            return element.bid.dateTime;
        }
    }

    /**
     * Enhanced aggregate function that preserves latency tracking information
     */
    private static final class CountBidsWithLatency implements AggregateFunction<BidWithTimestamp, CountBidsWithLatency.Accumulator, LatencyTrackedTuple> {

        private static class Accumulator {
            long auction = 0L;
            long count = 0L;
            long maxSourceEmitTime = 0L;  // Changed from min to max to measure processing latency
            long maxAfterTimestampTime = 0L;
            long maxBeforeWindowTime = 0L;
        }

        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }

        @Override
        public Accumulator add(BidWithTimestamp value, Accumulator accumulator) {
            accumulator.auction = value.bid.auction;
            accumulator.count += 1;
            // Track the latest source emit time (most recent element) to measure processing latency
            accumulator.maxSourceEmitTime = Math.max(accumulator.maxSourceEmitTime, value.sourceEmitTime);
            accumulator.maxAfterTimestampTime = Math.max(accumulator.maxAfterTimestampTime, value.afterTimestampTime);
            accumulator.maxBeforeWindowTime = Math.max(accumulator.maxBeforeWindowTime, value.beforeWindowTime);
            return accumulator;
        }

        @Override
        public LatencyTrackedTuple getResult(Accumulator accumulator) {
            LatencyTrackedTuple result = new LatencyTrackedTuple(new Tuple2<>(accumulator.auction, accumulator.count));
            result.setSourceEmitTime(accumulator.maxSourceEmitTime);
            result.setAfterTimestampTime(accumulator.maxAfterTimestampTime);
            result.setBeforeWindowTime(accumulator.maxBeforeWindowTime);
            return result;
        }

        @Override
        public Accumulator merge(Accumulator a, Accumulator b) {
            Accumulator merged = new Accumulator();
            merged.auction = a.auction; // They should be the same
            merged.count = a.count + b.count;
            merged.maxSourceEmitTime = Math.max(a.maxSourceEmitTime, b.maxSourceEmitTime);
            merged.maxAfterTimestampTime = Math.max(a.maxAfterTimestampTime, b.maxAfterTimestampTime);
            merged.maxBeforeWindowTime = Math.max(a.maxBeforeWindowTime, b.maxBeforeWindowTime);
            return merged;
        }
    }
}
