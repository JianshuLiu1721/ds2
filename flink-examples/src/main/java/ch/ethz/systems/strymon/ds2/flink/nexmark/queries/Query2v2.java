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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query2v2 {

    private static final Logger logger  = LoggerFactory.getLogger(Query2v2.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.disableOperatorChaining();

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        final int srcRate = params.getInt("srcRate", 100000);

        // Step 1: Add source timestamps to bids
        DataStream<BidWithTimestamp> bidsWithTimestamp = env.addSource(new BidSourceFunction(srcRate))
                .setParallelism(params.getInt("p-source", 1))
                .name("Bids Source")
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

        // Step 2: Before flatMap operation - add timestamp
        DataStream<BidWithTimestamp> beforeFlatMap = bidsWithTimestamp
                .map(new MapFunction<BidWithTimestamp, BidWithTimestamp>() {
                    @Override
                    public BidWithTimestamp map(BidWithTimestamp bwt) throws Exception {
                        bwt.beforeFlatMapTime = System.currentTimeMillis();
                        return bwt;
                    }
                })
                .name("Add Before FlatMap Time");

        // Step 3: Perform the flatMap operation (filter + convert) with latency tracking
        // SELECT Rstream(auction, price)
        // FROM Bid [NOW]
        // WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
        DataStream<FlatMapLatencyTrackedTuple> converted = beforeFlatMap
                .flatMap(new FlatMapFunction<BidWithTimestamp, FlatMapLatencyTrackedTuple>() {
                    @Override
                    public void flatMap(BidWithTimestamp bwt, Collector<FlatMapLatencyTrackedTuple> out) throws Exception {
                        // Apply the filter condition
                        if (bwt.bid.auction % 1007 == 0 || bwt.bid.auction % 1020 == 0 ||
                            bwt.bid.auction % 2001 == 0 || bwt.bid.auction % 2019 == 0 ||
                            bwt.bid.auction % 2087 == 0) {

                            Tuple2<Long, Long> result = new Tuple2<>(bwt.bid.auction, bwt.bid.price);

                            FlatMapLatencyTrackedTuple tracked = new FlatMapLatencyTrackedTuple(result, bwt.sourceEmitTime);
                            tracked.setBeforeFlatMapTime(bwt.beforeFlatMapTime);
                            tracked.setAfterFlatMapTime(System.currentTimeMillis());
                            out.collect(tracked);
                        }
                        // If filter condition fails, we don't emit anything (flatMap behavior)
                    }
                })
                .setParallelism(params.getInt("p-flatMap", 1))
                .name("FlatMap with Latency Tracking");

        // Step 4: Sink (will add sink receive time)
        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        converted.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-flatMap", 1));

        // execute program
        env.execute("Nexmark Query2v2");
    }

    /**
     * Wrapper class for Bid with source timestamp
     */
    private static class BidWithTimestamp {
        public Bid bid;
        public long sourceEmitTime;
        public long beforeFlatMapTime;
    }

    /**
     * Latency tracked tuple for flatMap operation results
     */
    public static class FlatMapLatencyTrackedTuple {
        public Tuple2<Long, Long> data;
        public long sourceEmitTime;        // When the bid was emitted from source
        public long beforeFlatMapTime;     // Before the flatMap operation
        public long afterFlatMapTime;      // After the flatMap operation
        public long sinkReceiveTime;       // When received at sink

        public FlatMapLatencyTrackedTuple() {
            this.data = new Tuple2<>();
        }

        public FlatMapLatencyTrackedTuple(Tuple2<Long, Long> data, long sourceEmitTime) {
            this.data = data;
            this.sourceEmitTime = sourceEmitTime;
        }

        public void setBeforeFlatMapTime(long time) {
            this.beforeFlatMapTime = time;
        }

        public void setAfterFlatMapTime(long time) {
            this.afterFlatMapTime = time;
        }

        public void setSinkReceiveTime(long time) {
            this.sinkReceiveTime = time;
        }

        public long getTotalLatency() {
            return sinkReceiveTime - sourceEmitTime;
        }

        public long getSourceToBeforeFlatMapLatency() {
            return beforeFlatMapTime - sourceEmitTime;
        }

        public long getFlatMapProcessingLatency() {
            return afterFlatMapTime - beforeFlatMapTime;
        }

        public long getAfterFlatMapToSinkLatency() {
            return sinkReceiveTime - afterFlatMapTime;
        }

        @Override
        public String toString() {
            return String.format("FlatMapLatencyTrackedTuple{data=%s, total=%dms, source->beforeFlatMap=%dms, flatMap=%dms, afterFlatMap->sink=%dms}",
                    data, getTotalLatency(), getSourceToBeforeFlatMapLatency(), getFlatMapProcessingLatency(), getAfterFlatMapToSinkLatency());
        }
    }
}
