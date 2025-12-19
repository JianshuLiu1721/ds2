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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class Query11v2 {

    private static final Logger logger = LoggerFactory.getLogger(Query11v2.class);

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

        // Step 1: Add source timestamps to bids
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
        // Session window with 10-second gap, custom trigger fires at 100k events
        DataStream<SessionWindowLatencyTrackedTuple> windowed = beforeWindow
                .keyBy(new KeySelector<BidWithTimestamp, Long>() {
                    @Override
                    public Long getKey(BidWithTimestamp bwt) throws Exception {
                        return bwt.bid.bidder;
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .trigger(new MaxLogEventsTrigger())
                .aggregate(new CountBidsPerSessionWithLatency())
                .setParallelism(params.getInt("p-window", 1))
                .name("Session Window");

        // Step 5: After window - add timestamp
        DataStream<SessionWindowLatencyTrackedTuple> afterWindow = windowed
                .map(new MapFunction<SessionWindowLatencyTrackedTuple, SessionWindowLatencyTrackedTuple>() {
                    @Override
                    public SessionWindowLatencyTrackedTuple map(SessionWindowLatencyTrackedTuple swlt)
                            throws Exception {
                        swlt.setAfterWindowTime(System.currentTimeMillis());
                        return swlt;
                    }
                })
                .name("Add After Window Time");

        // Step 6: Sink (will add sink receive time)
        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        afterWindow.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-window", 1));

        // execute program
        env.execute("Nexmark Query11v2");
    }

    /**
     * Wrapper class for Bid with timestamps
     */
    private static class BidWithTimestamp {
        public Bid bid;
        public long sourceEmitTime;
        public long afterTimestampTime;
        public long beforeWindowTime;
    }

    /**
     * Latency tracked tuple for session window results
     */
    public static class SessionWindowLatencyTrackedTuple {
        public Tuple2<Long, Long> data;
        public long sourceEmitTime; // When the latest bid in session was emitted
        public long afterTimestampTime; // After timestamp assignment
        public long beforeWindowTime; // Before entering window
        public long afterWindowTime; // After window computation
        public long sinkReceiveTime; // When received at sink

        public SessionWindowLatencyTrackedTuple() {
            this.data = new Tuple2<>();
        }

        public SessionWindowLatencyTrackedTuple(Tuple2<Long, Long> data, long sourceEmitTime) {
            this.data = data;
            this.sourceEmitTime = sourceEmitTime;
        }

        public void setAfterTimestampTime(long time) {
            this.afterTimestampTime = time;
        }

        public void setBeforeWindowTime(long time) {
            this.beforeWindowTime = time;
        }

        public void setAfterWindowTime(long time) {
            this.afterWindowTime = time;
        }

        public void setSinkReceiveTime(long time) {
            this.sinkReceiveTime = time;
        }

        public long getTotalLatency() {
            return sinkReceiveTime - sourceEmitTime;
        }

        public long getSourceToTimestampLatency() {
            return afterTimestampTime - sourceEmitTime;
        }

        public long getTimestampToWindowLatency() {
            return beforeWindowTime - afterTimestampTime;
        }

        public long getWindowProcessingLatency() {
            return afterWindowTime - beforeWindowTime;
        }

        public long getWindowToSinkLatency() {
            return sinkReceiveTime - afterWindowTime;
        }

        @Override
        public String toString() {
            return String.format(
                    "SessionWindowLatencyTrackedTuple{data=%s, total=%dms, source->ts=%dms, ts->window=%dms, window=%dms, window->sink=%dms}",
                    data, getTotalLatency(), getSourceToTimestampLatency(),
                    getTimestampToWindowLatency(), getWindowProcessingLatency(), getWindowToSinkLatency());
        }
    }

    /**
     * Custom trigger that fires when 100k events accumulate or on event time
     */
    private static final class MaxLogEventsTrigger extends Trigger<BidWithTimestamp, TimeWindow> {

        private final long maxEvents = 100000L;

        private final ReducingStateDescriptor<Long> stateDesc = new ReducingStateDescriptor<>("count", new Sum(),
                LongSerializer.INSTANCE);

        @Override
        public TriggerResult onElement(BidWithTimestamp element, long timestamp, TimeWindow window,
                Trigger.TriggerContext ctx) throws Exception {
            ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
            count.add(1L);
            if (count.get() >= maxEvents) {
                count.clear();
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, Trigger.TriggerContext ctx)
                throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
            ctx.mergePartitionedState(stateDesc);
        }

        @Override
        public void clear(TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(stateDesc).clear();
        }

        private static class Sum implements ReduceFunction<Long> {
            private static final long serialVersionUID = 1L;

            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }
        }
    }

    /**
     * Enhanced aggregate function that preserves latency tracking information
     * Tracks the earliest source emit time from bids in the session
     */
    private static final class CountBidsPerSessionWithLatency implements
            AggregateFunction<BidWithTimestamp, CountBidsPerSessionWithLatency.Accumulator, SessionWindowLatencyTrackedTuple> {

        private static class Accumulator {
            long bidId = 0L;
            long count = 0L;
            long maxSourceEmitTime = 0L; // Track latest bid in session for processing latency
            long maxAfterTimestampTime = 0L;
            long maxBeforeWindowTime = 0L;
        }

        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }

        @Override
        public Accumulator add(BidWithTimestamp value, Accumulator accumulator) {
            accumulator.bidId = value.bid.auction;
            accumulator.count += 1;
            // Track the latest source emit time (most recent element)
            accumulator.maxSourceEmitTime = Math.max(accumulator.maxSourceEmitTime, value.sourceEmitTime);
            accumulator.maxAfterTimestampTime = Math.max(accumulator.maxAfterTimestampTime, value.afterTimestampTime);
            accumulator.maxBeforeWindowTime = Math.max(accumulator.maxBeforeWindowTime, value.beforeWindowTime);
            return accumulator;
        }

        @Override
        public SessionWindowLatencyTrackedTuple getResult(Accumulator accumulator) {
            SessionWindowLatencyTrackedTuple result = new SessionWindowLatencyTrackedTuple(
                    new Tuple2<>(accumulator.bidId, accumulator.count),
                    accumulator.maxSourceEmitTime);
            result.setAfterTimestampTime(accumulator.maxAfterTimestampTime);
            result.setBeforeWindowTime(accumulator.maxBeforeWindowTime);
            return result;
        }

        @Override
        public Accumulator merge(Accumulator a, Accumulator b) {
            Accumulator merged = new Accumulator();
            merged.bidId = a.bidId; // They should be the same
            merged.count = a.count + b.count;
            merged.maxSourceEmitTime = Math.max(a.maxSourceEmitTime, b.maxSourceEmitTime);
            merged.maxAfterTimestampTime = Math.max(a.maxAfterTimestampTime, b.maxAfterTimestampTime);
            merged.maxBeforeWindowTime = Math.max(a.maxBeforeWindowTime, b.maxBeforeWindowTime);
            return merged;
        }
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
}
