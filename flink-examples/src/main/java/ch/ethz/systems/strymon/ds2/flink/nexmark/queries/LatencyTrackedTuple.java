package ch.ethz.systems.strymon.ds2.flink.nexmark.queries;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Wrapper class to track latency
 */
public class LatencyTrackedTuple {
    public Tuple2<Long, Long> data;

    // Timestamps at different stages (in milliseconds)
    public long sourceEmitTime;        // When event was emitted from source
    public long afterTimestampTime;    // After timestamp assignment
    public long beforeWindowTime;      // Before entering window
    public long afterWindowTime;       // After window computation
    public long sinkReceiveTime;       // When received at sink

    public LatencyTrackedTuple() {
        this.data = new Tuple2<>();
    }

    public LatencyTrackedTuple(Tuple2<Long, Long> data) {
        this.data = data;
    }

    public void setSourceEmitTime(long time) {
        this.sourceEmitTime = time;
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

    // Calculate latencies between stages
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

    public long getTotalLatency() {
        return sinkReceiveTime - sourceEmitTime;
    }

    @Override
    public String toString() {
        return String.format("LatencyTrackedTuple{data=%s, total=%dms, source->ts=%dms, ts->window=%dms, window=%dms, window->sink=%dms}",
                data, getTotalLatency(), getSourceToTimestampLatency(),
                getTimestampToWindowLatency(), getWindowProcessingLatency(), getWindowToSinkLatency());
    }
}
