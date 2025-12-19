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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query1v2 {

    private static final Logger logger  = LoggerFactory.getLogger(Query1v2.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        final float exchangeRate = params.getFloat("exchange-rate", 0.82F);

        final int srcRate = params.getInt("srcRate", 100000);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.disableOperatorChaining();

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        // Step 1: Add source timestamps to bids
        DataStream<BidWithTimestamp> bidsWithTimestamp = env.addSource(new BidSourceFunction(srcRate))
                .setParallelism(params.getInt("p-source", 1))
                .name("Bids Source")
                .uid("Bids-Source")
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

        // Step 2: Before map operation - add timestamp
        DataStream<BidWithTimestamp> beforeMap = bidsWithTimestamp
                .map(new MapFunction<BidWithTimestamp, BidWithTimestamp>() {
                    @Override
                    public BidWithTimestamp map(BidWithTimestamp bwt) throws Exception {
                        bwt.beforeMapTime = System.currentTimeMillis();
                        return bwt;
                    }
                })
                .name("Add Before Map Time");

        // Step 3: Perform the map operation (dollar to euro conversion) with latency tracking
        // SELECT auction, DOLTOEUR(price), bidder, datetime
        DataStream<MapLatencyTrackedTuple> mapped = beforeMap.map(new MapFunction<BidWithTimestamp, MapLatencyTrackedTuple>() {
            @Override
            public MapLatencyTrackedTuple map(BidWithTimestamp bwt) throws Exception {
                Tuple4<Long, Long, Long, Long> result = new Tuple4<>(
                    bwt.bid.auction,
                    dollarToEuro(bwt.bid.price, exchangeRate),
                    bwt.bid.bidder,
                    bwt.bid.dateTime
                );

                MapLatencyTrackedTuple tracked = new MapLatencyTrackedTuple(result, bwt.sourceEmitTime);
                tracked.setBeforeMapTime(bwt.beforeMapTime);
                tracked.setAfterMapTime(System.currentTimeMillis());
                return tracked;
            }
        }).setParallelism(params.getInt("p-map", 1))
                .name("Map with Latency Tracking")
                .uid("Mapper");

        // Step 4: Sink (will add sink receive time)
        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        mapped.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-map", 1))
                .name("Latency Sink")
                .uid("Latency-Sink");

        // execute program
        env.execute("Nexmark Query1v2");
    }

    private static long dollarToEuro(long dollarPrice, float rate) {
        return (long) (rate * dollarPrice);
    }

    /**
     * Wrapper class for Bid with source timestamp
     */
    private static class BidWithTimestamp {
        public Bid bid;
        public long sourceEmitTime;
        public long beforeMapTime;
    }

    /**
     * Latency tracked tuple for map operation results
     */
    public static class MapLatencyTrackedTuple {
        public Tuple4<Long, Long, Long, Long> data;
        public long sourceEmitTime;        // When the bid was emitted from source
        public long beforeMapTime;         // Before the map operation
        public long afterMapTime;          // After the map operation
        public long sinkReceiveTime;       // When received at sink

        public MapLatencyTrackedTuple() {
            this.data = new Tuple4<>();
        }

        public MapLatencyTrackedTuple(Tuple4<Long, Long, Long, Long> data, long sourceEmitTime) {
            this.data = data;
            this.sourceEmitTime = sourceEmitTime;
        }

        public void setBeforeMapTime(long time) {
            this.beforeMapTime = time;
        }

        public void setAfterMapTime(long time) {
            this.afterMapTime = time;
        }

        public void setSinkReceiveTime(long time) {
            this.sinkReceiveTime = time;
        }

        public long getTotalLatency() {
            return sinkReceiveTime - sourceEmitTime;
        }

        public long getSourceToBeforeMapLatency() {
            return beforeMapTime - sourceEmitTime;
        }

        public long getMapProcessingLatency() {
            return afterMapTime - beforeMapTime;
        }

        public long getAfterMapToSinkLatency() {
            return sinkReceiveTime - afterMapTime;
        }

        @Override
        public String toString() {
            return String.format("MapLatencyTrackedTuple{data=%s, total=%dms, source->beforeMap=%dms, map=%dms, afterMap->sink=%dms}",
                    data, getTotalLatency(), getSourceToBeforeMapLatency(), getMapProcessingLatency(), getAfterMapToSinkLatency());
        }
    }
}