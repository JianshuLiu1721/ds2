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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;

public class Query3v2 {

    private static final Logger logger = LoggerFactory.getLogger(Query3v2.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable latency tracking
        // env.getConfig().setLatencyTrackingInterval(5000);

        env.disableOperatorChaining();

        final int auctionSrcRate = params.getInt("auction-srcRate", 20000);

        final int personSrcRate = params.getInt("person-srcRate", 10000);

        // Add source timestamps for auctions
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

        // Add source timestamps for persons
        DataStream<PersonWithTimestamp> personsWithTimestamp = env.addSource(new PersonSourceFunction(personSrcRate))
                .name("Custom Source: Persons")
                .setParallelism(params.getInt("p-person-source", 1))
                .filter(new FilterFunction<Person>() {
                    @Override
                    public boolean filter(Person person) throws Exception {
                        return (person.state.equals("OR") || person.state.equals("ID") || person.state.equals("CA"));
                    }
                })
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

        // Add before join timestamps
        DataStream<AuctionWithTimestamp> auctionsBeforeJoin = auctionsWithTimestamp
                .map(new MapFunction<AuctionWithTimestamp, AuctionWithTimestamp>() {
                    @Override
                    public AuctionWithTimestamp map(AuctionWithTimestamp awt) throws Exception {
                        awt.beforeJoinTime = System.currentTimeMillis();
                        return awt;
                    }
                })
                .name("Add Auction Before Join Time");

        DataStream<PersonWithTimestamp> personsBeforeJoin = personsWithTimestamp
                .map(new MapFunction<PersonWithTimestamp, PersonWithTimestamp>() {
                    @Override
                    public PersonWithTimestamp map(PersonWithTimestamp pwt) throws Exception {
                        pwt.beforeJoinTime = System.currentTimeMillis();
                        return pwt;
                    }
                })
                .name("Add Person Before Join Time");

        // SELECT Istream(P.name, P.city, P.state, A.id)
        // FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
        // WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state =
        // `CA')

        KeyedStream<AuctionWithTimestamp, Long> keyedAuctions = auctionsBeforeJoin
                .keyBy(new KeySelector<AuctionWithTimestamp, Long>() {
                    @Override
                    public Long getKey(AuctionWithTimestamp awt) throws Exception {
                        return awt.auction.seller;
                    }
                });

        KeyedStream<PersonWithTimestamp, Long> keyedPersons = personsBeforeJoin
                .keyBy(new KeySelector<PersonWithTimestamp, Long>() {
                    @Override
                    public Long getKey(PersonWithTimestamp pwt) throws Exception {
                        return pwt.person.id;
                    }
                });

        DataStream<JoinLatencyTrackedTuple> joined = keyedAuctions.connect(keyedPersons)
                .flatMap(new JoinPersonsWithAuctionsTracked()).name("Incremental join")
                .setParallelism(params.getInt("p-join", 1));

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        joined.transform("Sink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-join", 1));

        // execute program
        env.execute("Nexmark Query3v2");
    }

    /**
     * Wrapper class for Auction with source timestamp
     */
    private static class AuctionWithTimestamp {
        public Auction auction;
        public long sourceEmitTime;
        public long beforeJoinTime;
    }

    /**
     * Wrapper class for Person with source timestamp
     */
    private static class PersonWithTimestamp {
        public Person person;
        public long sourceEmitTime;
        public long beforeJoinTime;
    }

    /**
     * Latency tracked tuple for join results
     */
    public static class JoinLatencyTrackedTuple {
        public Tuple4<String, String, String, Long> data;
        public long sourceEmitTime; // When the first event (auction or person) was emitted
        public long beforeJoinTime; // Before entering the join (before keyBy)
        public long afterJoinTime; // When the join match occurred
        public long sinkReceiveTime; // When received at sink

        public JoinLatencyTrackedTuple() {
            this.data = new Tuple4<>();
        }

        public JoinLatencyTrackedTuple(Tuple4<String, String, String, Long> data, long sourceEmitTime) {
            this.data = data;
            this.sourceEmitTime = sourceEmitTime;
            this.afterJoinTime = System.currentTimeMillis();
        }

        public void setBeforeJoinTime(long time) {
            this.beforeJoinTime = time;
        }

        public void setSinkReceiveTime(long time) {
            this.sinkReceiveTime = time;
        }

        public long getTotalLatency() {
            return sinkReceiveTime - sourceEmitTime;
        }

        public long getSourceToBeforeJoinLatency() {
            return beforeJoinTime - sourceEmitTime;
        }

        public long getJoinProcessingLatency() {
            return afterJoinTime - beforeJoinTime;
        }

        public long getAfterJoinToSinkLatency() {
            return sinkReceiveTime - afterJoinTime;
        }

        @Override
        public String toString() {
            return String.format(
                    "JoinLatencyTrackedTuple{data=%s, total=%dms, source->beforeJoin=%dms, join=%dms, afterJoin->sink=%dms}",
                    data, getTotalLatency(), getSourceToBeforeJoinLatency(), getJoinProcessingLatency(),
                    getAfterJoinToSinkLatency());
        }
    }

    private static final class JoinPersonsWithAuctionsTracked
            extends RichCoFlatMapFunction<AuctionWithTimestamp, PersonWithTimestamp, JoinLatencyTrackedTuple> {

        // person state: id, <name, city, state, sourceEmitTime>
        private HashMap<Long, Tuple4<String, String, String, Long>> personMap = new HashMap<>();

        // auction state: seller, List<(id, sourceEmitTime)>
        private HashMap<Long, HashSet<Tuple2<Long, Long>>> auctionMap = new HashMap<>();

        @Override
        public void flatMap1(AuctionWithTimestamp awt, Collector<JoinLatencyTrackedTuple> out) throws Exception {
            // check if auction has a match in the person state
            if (personMap.containsKey(awt.auction.seller)) {
                // emit and don't store
                Tuple4<String, String, String, Long> match = personMap.get(awt.auction.seller);
                Tuple4<String, String, String, Long> result = new Tuple4<>(match.f0, match.f1, match.f2,
                        awt.auction.id);
                // Use auction's emit time since it's the triggering event for this join output
                JoinLatencyTrackedTuple tracked = new JoinLatencyTrackedTuple(result, awt.sourceEmitTime);
                tracked.setBeforeJoinTime(awt.beforeJoinTime);
                out.collect(tracked);
            } else {
                // we need to store this auction for future matches
                if (auctionMap.containsKey(awt.auction.seller)) {
                    HashSet<Tuple2<Long, Long>> ids = auctionMap.get(awt.auction.seller);
                    ids.add(new Tuple2<>(awt.auction.id, awt.sourceEmitTime));
                    auctionMap.put(awt.auction.seller, ids);
                } else {
                    HashSet<Tuple2<Long, Long>> ids = new HashSet<>();
                    ids.add(new Tuple2<>(awt.auction.id, awt.sourceEmitTime));
                    auctionMap.put(awt.auction.seller, ids);
                }
            }
        }

        @Override
        public void flatMap2(PersonWithTimestamp pwt, Collector<JoinLatencyTrackedTuple> out) throws Exception {
            // store person in state with timestamp
            personMap.put(pwt.person.id,
                    new Tuple4<>(pwt.person.name, pwt.person.city, pwt.person.state, pwt.sourceEmitTime));

            // check if person has a match in the auction state
            if (auctionMap.containsKey(pwt.person.id)) {
                // output all matches and remove
                HashSet<Tuple2<Long, Long>> auctionData = auctionMap.remove(pwt.person.id);
                for (Tuple2<Long, Long> auctionInfo : auctionData) {
                    Long auctionId = auctionInfo.f0;
                    Long auctionEmitTime = auctionInfo.f1;
                    Tuple4<String, String, String, Long> result = new Tuple4<>(pwt.person.name, pwt.person.city,
                            pwt.person.state, auctionId);
                    // Use person's emit time since it's the triggering event for this join output
                    JoinLatencyTrackedTuple tracked = new JoinLatencyTrackedTuple(result, pwt.sourceEmitTime);
                    tracked.setBeforeJoinTime(pwt.beforeJoinTime);
                    out.collect(tracked);
                }
            }
        }
    }

}
