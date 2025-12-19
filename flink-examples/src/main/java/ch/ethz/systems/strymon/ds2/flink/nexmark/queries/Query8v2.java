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
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.AuctionSourceFunction;
import ch.ethz.systems.strymon.ds2.flink.nexmark.sources.PersonSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class Query8v2 {

    private static final Logger logger = LoggerFactory.getLogger(Query8v2.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        final int auctionSrcRate = params.getInt("auction-srcRate", 50000);

        final int personSrcRate = params.getInt("person-srcRate", 30000);

        env.setParallelism(params.getInt("p-window", 1));

        // Step 1: Add source timestamps to persons
        DataStream<PersonWithTimestamp> personsWithTimestamp = env.addSource(new PersonSourceFunction(personSrcRate))
                .name("Custom Source: Persons")
                .setParallelism(params.getInt("p-person-source", 1))
                .map(new MapFunction<Person, PersonWithTimestamp>() {
                    @Override
                    public PersonWithTimestamp map(Person person) throws Exception {
                        PersonWithTimestamp pwt = new PersonWithTimestamp();
                        pwt.person = person;
                        pwt.sourceEmitTime = System.currentTimeMillis();
                        return pwt;
                    }
                })
                .name("Add Person Source Timestamp");

        // Step 2: Assign watermarks and add timestamp
        DataStream<PersonWithTimestamp> personsTimestamped = personsWithTimestamp
                .assignTimestampsAndWatermarks(new PersonTimestampAssigner())
                .map(new MapFunction<PersonWithTimestamp, PersonWithTimestamp>() {
                    @Override
                    public PersonWithTimestamp map(PersonWithTimestamp pwt) throws Exception {
                        pwt.afterTimestampTime = System.currentTimeMillis();
                        return pwt;
                    }
                })
                .name("Add Person After Timestamp Time");

        // Step 3: Add source timestamps to auctions
        DataStream<AuctionWithTimestamp> auctionsWithTimestamp = env
                .addSource(new AuctionSourceFunction(auctionSrcRate))
                .name("Custom Source: Auctions")
                .setParallelism(params.getInt("p-auction-source", 1))
                .map(new MapFunction<Auction, AuctionWithTimestamp>() {
                    @Override
                    public AuctionWithTimestamp map(Auction auction) throws Exception {
                        AuctionWithTimestamp awt = new AuctionWithTimestamp();
                        awt.auction = auction;
                        awt.sourceEmitTime = System.currentTimeMillis();
                        return awt;
                    }
                })
                .name("Add Auction Source Timestamp");

        // Step 4: Assign watermarks and add timestamp
        DataStream<AuctionWithTimestamp> auctionsTimestamped = auctionsWithTimestamp
                .assignTimestampsAndWatermarks(new AuctionTimestampAssigner())
                .map(new MapFunction<AuctionWithTimestamp, AuctionWithTimestamp>() {
                    @Override
                    public AuctionWithTimestamp map(AuctionWithTimestamp awt) throws Exception {
                        awt.afterTimestampTime = System.currentTimeMillis();
                        return awt;
                    }
                })
                .name("Add Auction After Timestamp Time");

        // Step 4a: Add before window timestamp
        DataStream<PersonWithTimestamp> personsBeforeWindow = personsTimestamped
                .map(new MapFunction<PersonWithTimestamp, PersonWithTimestamp>() {
                    @Override
                    public PersonWithTimestamp map(PersonWithTimestamp pwt) throws Exception {
                        pwt.beforeWindowTime = System.currentTimeMillis();
                        return pwt;
                    }
                })
                .name("Add Person Before Window Time");

        DataStream<AuctionWithTimestamp> auctionsBeforeWindow = auctionsTimestamped
                .map(new MapFunction<AuctionWithTimestamp, AuctionWithTimestamp>() {
                    @Override
                    public AuctionWithTimestamp map(AuctionWithTimestamp awt) throws Exception {
                        awt.beforeWindowTime = System.currentTimeMillis();
                        return awt;
                    }
                })
                .name("Add Auction Before Window Time");

        // Step 5: Perform windowed join with latency tracking
        // SELECT Rstream(P.id, P.name, A.reserve)
        // FROM Person [RANGE 1 HOUR] P, Auction [RANGE 1 HOUR] A
        // WHERE P.id = A.seller;
        DataStream<WindowedJoinLatencyTrackedTuple> joined = personsBeforeWindow.join(auctionsBeforeWindow)
                .where(new KeySelector<PersonWithTimestamp, Long>() {
                    @Override
                    public Long getKey(PersonWithTimestamp pwt) {
                        return pwt.person.id;
                    }
                }).equalTo(new KeySelector<AuctionWithTimestamp, Long>() {
                    @Override
                    public Long getKey(AuctionWithTimestamp awt) {
                        return awt.auction.seller;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new FlatJoinFunction<PersonWithTimestamp, AuctionWithTimestamp, WindowedJoinLatencyTrackedTuple>() {
                    @Override
                    public void join(PersonWithTimestamp pwt, AuctionWithTimestamp awt,
                            Collector<WindowedJoinLatencyTrackedTuple> out) {
                        Tuple3<Long, String, Long> result = new Tuple3<>(pwt.person.id, pwt.person.name,
                                awt.auction.reserve);

                        // Use the latest source emit time from both streams (later-arriving element)
                        long maxSourceEmitTime = Math.max(pwt.sourceEmitTime, awt.sourceEmitTime);
                        long maxAfterTimestampTime = Math.max(pwt.afterTimestampTime, awt.afterTimestampTime);
                        long maxBeforeWindowTime = Math.max(pwt.beforeWindowTime, awt.beforeWindowTime);

                        WindowedJoinLatencyTrackedTuple tracked = new WindowedJoinLatencyTrackedTuple(result,
                                maxSourceEmitTime);
                        tracked.setAfterTimestampTime(maxAfterTimestampTime);
                        tracked.setBeforeWindowTime(maxBeforeWindowTime);
                        tracked.setWindowJoinTime(System.currentTimeMillis());
                        out.collect(tracked);
                    }
                });

        // Step 6: Add after window timestamp
        DataStream<WindowedJoinLatencyTrackedTuple> afterWindow = joined
                .map(new MapFunction<WindowedJoinLatencyTrackedTuple, WindowedJoinLatencyTrackedTuple>() {
                    @Override
                    public WindowedJoinLatencyTrackedTuple map(WindowedJoinLatencyTrackedTuple tuple) throws Exception {
                        tuple.setAfterWindowTime(System.currentTimeMillis());
                        return tuple;
                    }
                })
                .name("Add After Window Time");

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        afterWindow.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-window", 1));

        // execute program
        env.execute("Nexmark Query8v2");
    }

    /**
     * Wrapper class for Person with timestamps
     */
    private static class PersonWithTimestamp {
        public Person person;
        public long sourceEmitTime;
        public long afterTimestampTime;
        public long beforeWindowTime;
    }

    /**
     * Wrapper class for Auction with timestamps
     */
    private static class AuctionWithTimestamp {
        public Auction auction;
        public long sourceEmitTime;
        public long afterTimestampTime;
        public long beforeWindowTime;
    }

    /**
     * Latency tracked tuple for windowed join results
     */
    public static class WindowedJoinLatencyTrackedTuple {
        public Tuple3<Long, String, Long> data;
        public long sourceEmitTime; // When the first event (person or auction) was emitted
        public long afterTimestampTime; // After timestamp assignment
        public long beforeWindowTime; // Before entering window
        public long windowJoinTime; // When the window join occurred
        public long afterWindowTime; // After window completes
        public long sinkReceiveTime; // When received at sink

        public WindowedJoinLatencyTrackedTuple() {
            this.data = new Tuple3<>();
        }

        public WindowedJoinLatencyTrackedTuple(Tuple3<Long, String, Long> data, long sourceEmitTime) {
            this.data = data;
            this.sourceEmitTime = sourceEmitTime;
        }

        public void setAfterTimestampTime(long time) {
            this.afterTimestampTime = time;
        }

        public void setBeforeWindowTime(long time) {
            this.beforeWindowTime = time;
        }

        public void setWindowJoinTime(long time) {
            this.windowJoinTime = time;
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
                    "WindowedJoinLatencyTrackedTuple{data=%s, total=%dms, source->ts=%dms, ts->window=%dms, window=%dms, window->sink=%dms}",
                    data, getTotalLatency(), getSourceToTimestampLatency(), getTimestampToWindowLatency(),
                    getWindowProcessingLatency(), getWindowToSinkLatency());
        }
    }

    private static final class PersonTimestampAssigner implements AssignerWithPeriodicWatermarks<PersonWithTimestamp> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(PersonWithTimestamp element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.person.dateTime);
            return element.person.dateTime;
        }
    }

    private static final class AuctionTimestampAssigner
            implements AssignerWithPeriodicWatermarks<AuctionWithTimestamp> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(AuctionWithTimestamp element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.auction.dateTime);
            return element.auction.dateTime;
        }
    }

}
