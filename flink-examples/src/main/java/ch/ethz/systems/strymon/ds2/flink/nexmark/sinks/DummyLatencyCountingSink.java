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

package ch.ethz.systems.strymon.ds2.flink.nexmark.sinks;

import ch.ethz.systems.strymon.ds2.flink.nexmark.queries.LatencyTrackedTuple;
import ch.ethz.systems.strymon.ds2.flink.nexmark.queries.Query1v2;
import ch.ethz.systems.strymon.ds2.flink.nexmark.queries.Query2v2;
import ch.ethz.systems.strymon.ds2.flink.nexmark.queries.Query3v2;
import ch.ethz.systems.strymon.ds2.flink.nexmark.queries.Query8v2;
import ch.ethz.systems.strymon.ds2.flink.nexmark.queries.Query11v2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;

import org.slf4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class DummyLatencyCountingSink<T> extends StreamSink<T> {

    private final Logger logger;

    // Query type enumeration for better type safety
    private enum QueryType {
        QUERY1V2("q1v2",
                "timestamp,event_type,total_latency_ms,source_to_before_map_ms,map_processing_ms,after_map_to_sink_ms,source_emit_time,sink_receive_time,data"),
        QUERY2V2("q2v2",
                "timestamp,event_type,total_latency_ms,source_to_before_flatmap_ms,flatmap_processing_ms,after_flatmap_to_sink_ms,source_emit_time,sink_receive_time,data"),
        QUERY3V2("q3v2",
                "timestamp,event_type,total_latency_ms,source_to_before_join_ms,join_processing_ms,after_join_to_sink_ms,source_emit_time,sink_receive_time,data"),
        QUERY5V2("q5v2",
                "timestamp,event_type,total_latency_ms,source_to_timestamp_ms,timestamp_to_window_ms,window_processing_ms,window_to_sink_ms,source_emit_time,sink_receive_time,data"),
        QUERY8V2("q8v2",
                "timestamp,event_type,total_latency_ms,source_to_timestamp_ms,timestamp_to_window_ms,window_processing_ms,window_to_sink_ms,source_emit_time,sink_receive_time,data"),
        QUERY11V2("q11v2",
                "timestamp,event_type,total_latency_ms,source_to_timestamp_ms,timestamp_to_window_ms,window_processing_ms,window_to_sink_ms,source_emit_time,sink_receive_time,data");

        private final String filePrefix;
        private final String header;

        QueryType(String filePrefix, String header) {
            this.filePrefix = filePrefix;
            this.header = header;
        }

        public String getFilePrefix() {
            return filePrefix;
        }

        public String getHeader() {
            return header;
        }
    }

    /**
     * CSV Writer wrapper for cleaner file management
     */
    private static class CSVWriter {
        private final PrintWriter writer;
        private final String filename;
        private int lineCount = 0;

        CSVWriter(String filename, String header) throws IOException {
            this.filename = filename;
            this.writer = new PrintWriter(new FileWriter(filename, false));
            writer.println(header);
            writer.flush();
        }

        void writeLine(String line) {
            if (writer != null) {
                writer.println(line);
                lineCount++;
                if (lineCount % 5000 == 0) {
                    writer.flush();
                }
            }
        }

        void close() {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        }
    }

    public DummyLatencyCountingSink(Logger log) {
        super(new LatencyCountingSinkFunction<T>());
        this.logger = log;
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        logger.warn("%{}%{}%{}%{}%{}%{}", "latency",
                System.currentTimeMillis() - latencyMarker.getMarkedTime(),
                System.currentTimeMillis(),
                latencyMarker.getMarkedTime(),
                latencyMarker.getSubtaskIndex(),
                getRuntimeContext().getIndexOfThisSubtask());
    }

    /**
     * Rich sink function that has access to RuntimeContext for subtask index
     */
    private static class LatencyCountingSinkFunction<T> extends RichSinkFunction<T> {
        private transient CSVWriter csvWriter;
        private transient int subtaskIndex;
        private transient QueryType queryType;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public void invoke(T value, Context ctx) throws Exception {
            if (value instanceof Query1v2.MapLatencyTrackedTuple) {
                if (csvWriter == null) {
                    queryType = QueryType.QUERY1V2;
                    csvWriter = new CSVWriter(queryType.getFilePrefix() + "_" + subtaskIndex + ".csv", queryType.getHeader());
                }
                handleMapLatency((Query1v2.MapLatencyTrackedTuple) value);
            } else if (value instanceof Query2v2.FlatMapLatencyTrackedTuple) {
                if (csvWriter == null) {
                    queryType = QueryType.QUERY2V2;
                    csvWriter = new CSVWriter(queryType.getFilePrefix() + "_" + subtaskIndex + ".csv", queryType.getHeader());
                }
                handleFlatMapLatency((Query2v2.FlatMapLatencyTrackedTuple) value);
            } else if (value instanceof Query3v2.JoinLatencyTrackedTuple) {
                if (csvWriter == null) {
                    queryType = QueryType.QUERY3V2;
                    csvWriter = new CSVWriter(queryType.getFilePrefix() + "_" + subtaskIndex + ".csv", queryType.getHeader());
                }
                handleJoinLatency((Query3v2.JoinLatencyTrackedTuple) value);
            } else if (value instanceof Query11v2.SessionWindowLatencyTrackedTuple) {
                if (csvWriter == null) {
                    queryType = QueryType.QUERY11V2;
                    csvWriter = new CSVWriter(queryType.getFilePrefix() + "_" + subtaskIndex + ".csv", queryType.getHeader());
                }
                handleSessionWindowLatency((Query11v2.SessionWindowLatencyTrackedTuple) value);
            } else if (value instanceof Query8v2.WindowedJoinLatencyTrackedTuple) {
                if (csvWriter == null) {
                    queryType = QueryType.QUERY8V2;
                    csvWriter = new CSVWriter(queryType.getFilePrefix() + "_" + subtaskIndex + ".csv", queryType.getHeader());
                }
                handleWindowedJoinLatency((Query8v2.WindowedJoinLatencyTrackedTuple) value);
            } else if (value instanceof LatencyTrackedTuple) {
                if (csvWriter == null) {
                    queryType = QueryType.QUERY5V2;
                    csvWriter = new CSVWriter(queryType.getFilePrefix() + "_" + subtaskIndex + ".csv", queryType.getHeader());
                }
                handleWindowedLatency((LatencyTrackedTuple) value);
            }
        }

        private void handleMapLatency(Query1v2.MapLatencyTrackedTuple tracked) {
            tracked.setSinkReceiveTime(System.currentTimeMillis());

            String line = String.format("%d,map,%d,%d,%d,%d,%d,%d,\"%s\"",
                    System.currentTimeMillis(),
                    tracked.getTotalLatency(),
                    tracked.getSourceToBeforeMapLatency(),
                    tracked.getMapProcessingLatency(),
                    tracked.getAfterMapToSinkLatency(),
                    tracked.sourceEmitTime,
                    tracked.sinkReceiveTime,
                    escapeCSV(tracked.data.toString()));

            if (csvWriter != null) {
                csvWriter.writeLine(line);
            }
        }

        private void handleFlatMapLatency(Query2v2.FlatMapLatencyTrackedTuple tracked) {
            tracked.setSinkReceiveTime(System.currentTimeMillis());

            String line = String.format("%d,flatmap,%d,%d,%d,%d,%d,%d,\"%s\"",
                    System.currentTimeMillis(),
                    tracked.getTotalLatency(),
                    tracked.getSourceToBeforeFlatMapLatency(),
                    tracked.getFlatMapProcessingLatency(),
                    tracked.getAfterFlatMapToSinkLatency(),
                    tracked.sourceEmitTime,
                    tracked.sinkReceiveTime,
                    escapeCSV(tracked.data.toString()));

            if (csvWriter != null) {
                csvWriter.writeLine(line);
            }
        }

        private void handleJoinLatency(Query3v2.JoinLatencyTrackedTuple tracked) {
            tracked.setSinkReceiveTime(System.currentTimeMillis());

            String line = String.format("%d,join,%d,%d,%d,%d,%d,%d,\"%s\"",
                    System.currentTimeMillis(),
                    tracked.getTotalLatency(),
                    tracked.getSourceToBeforeJoinLatency(),
                    tracked.getJoinProcessingLatency(),
                    tracked.getAfterJoinToSinkLatency(),
                    tracked.sourceEmitTime,
                    tracked.sinkReceiveTime,
                    escapeCSV(tracked.data.toString()));

            if (csvWriter != null) {
                csvWriter.writeLine(line);
            }
        }

        private void handleSessionWindowLatency(Query11v2.SessionWindowLatencyTrackedTuple tracked) {
            tracked.setSinkReceiveTime(System.currentTimeMillis());

            String line = String.format("%d,session_window,%d,%d,%d,%d,%d,%d,%d,\"%s\"",
                    System.currentTimeMillis(),
                    tracked.getTotalLatency(),
                    tracked.getSourceToTimestampLatency(),
                    tracked.getTimestampToWindowLatency(),
                    tracked.getWindowProcessingLatency(),
                    tracked.getWindowToSinkLatency(),
                    tracked.sourceEmitTime,
                    tracked.sinkReceiveTime,
                    escapeCSV(tracked.data.toString()));

            if (csvWriter != null) {
                csvWriter.writeLine(line);
            }
        }

        private void handleWindowedLatency(LatencyTrackedTuple tracked) {
            tracked.setSinkReceiveTime(System.currentTimeMillis());

            String line = String.format("%d,windowed,%d,%d,%d,%d,%d,%d,%d,\"%s\"",
                    System.currentTimeMillis(),
                    tracked.getTotalLatency(),
                    tracked.getSourceToTimestampLatency(),
                    tracked.getTimestampToWindowLatency(),
                    tracked.getWindowProcessingLatency(),
                    tracked.getWindowToSinkLatency(),
                    tracked.sourceEmitTime,
                    tracked.sinkReceiveTime,
                    escapeCSV(tracked.data.toString()));

            if (csvWriter != null) {
                csvWriter.writeLine(line);
            }
        }

        private void handleWindowedJoinLatency(Query8v2.WindowedJoinLatencyTrackedTuple tracked) {
            tracked.setSinkReceiveTime(System.currentTimeMillis());

            String line = String.format("%d,windowed_join,%d,%d,%d,%d,%d,%d,%d,\"%s\"",
                    System.currentTimeMillis(),
                    tracked.getTotalLatency(),
                    tracked.getSourceToTimestampLatency(),
                    tracked.getTimestampToWindowLatency(),
                    tracked.getWindowProcessingLatency(),
                    tracked.getWindowToSinkLatency(),
                    tracked.sourceEmitTime,
                    tracked.sinkReceiveTime,
                    escapeCSV(tracked.data.toString()));

            if (csvWriter != null) {
                csvWriter.writeLine(line);
            }
        }

        private String escapeCSV(String value) {
            return value.replace("\"", "\"\"");
        }

        @Override
        public void close() throws Exception {
            if (csvWriter != null) {
                csvWriter.close();
                csvWriter = null;
            }
            super.close();
        }
    }
}
