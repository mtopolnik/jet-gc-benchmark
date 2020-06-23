package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import org.HdrHistogram.Histogram;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StreamingRound2 {
    private static final long EVENTS_PER_SECOND = 1_000_000;
    private static final long NUM_KEYS = 185_000;
    private static final long WIN_SIZE_MILLIS = 10_000;
    private static final long SLIDING_STEP_MILLIS = 10;

    private static final long WARMUP_TIME_MILLIS = SECONDS.toMillis(20);
    private static final long MEASUREMENT_TIME_MILLIS = MINUTES.toMillis(4);
    private static final long TOTAL_TIME_MILLIS = WARMUP_TIME_MILLIS + MEASUREMENT_TIME_MILLIS;

    private static final long DIAGNOSTIC_KEYSET_DOWNSAMPLING_FACTOR = 1_000;
    private static final long SOURCE_THROUGHPUT_REPORTING_PERIOD_SECONDS = 1;
    private static final long SIMPLE_TIME_SPAN_MILLIS = HOURS.toMillis(3);

    public static void main(String[] args) {
        System.out.printf(
                "%,d milliseconds warmup%n" +
                "%,d milliseconds measurement%n" +
                "%,d events per second%n" +
                "%,d keys%n" +
                "%,d milliseconds sliding window%n" +
                "%,d milliseconds sliding step%n",
                WARMUP_TIME_MILLIS, MEASUREMENT_TIME_MILLIS,
                EVENTS_PER_SECOND, NUM_KEYS, WIN_SIZE_MILLIS, SLIDING_STEP_MILLIS
        );
        Pipeline pipeline = buildPipeline();
        JetInstance jet = Jet.bootstrappedInstance();
        Job job = jet.newJob(pipeline);
        Runtime.getRuntime().addShutdownHook(new Thread(job::cancel));
        job.join();
    }

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        StreamStage<Long> source = p.readFrom(longSource(EVENTS_PER_SECOND))
                                    .withNativeTimestamps(0)
                                    .rebalance();
        StreamStage<Tuple2<Long, Long>> latencies = source
                .groupingKey(n -> n % NUM_KEYS)
                .window(sliding(WIN_SIZE_MILLIS, SLIDING_STEP_MILLIS))
                .aggregate(counting())
                .filter(kwr -> kwr.getKey() % DIAGNOSTIC_KEYSET_DOWNSAMPLING_FACTOR == 0)
                .mapStateful(DetermineLatency::new, DetermineLatency::map);

        latencies.filter(t2 -> t2.f0() < TOTAL_TIME_MILLIS)
                 .map(t2 -> String.format("%d,%d", t2.f0(), t2.f1()))
                 .writeTo(Sinks.files("/home/ec2-user/laten"));
        latencies
              .mapStateful(RecordLatencyHistogram::new, RecordLatencyHistogram::map)
              .writeTo(Sinks.files("/home/ec2-user/bench"));

        return p;
    }

    private static class DetermineLatency {
        private long startTimestamp;
        private long lastTimestamp;

        Tuple2<Long, Long> map(KeyedWindowResult<Long, Long> kwr) {
            long timestamp = kwr.end();
            if (timestamp <= lastTimestamp) {
                return null;
            }
            if (lastTimestamp == 0) {
                startTimestamp = timestamp;
            }
            lastTimestamp = timestamp;

            long latency = System.currentTimeMillis() - timestamp;
            long count = kwr.result();
            if (latency == -1) { // very low latencies may be reported as negative due to clock skew
                latency = 0;
            }
            if (latency < 0) {
                throw new RuntimeException("Negative latency: " + latency);
            }

            System.out.format("time %,d: latency %,d ms, key %,d, count %,d%n",
                    simpleTime(timestamp), latency, kwr.getKey(), count);
            return tuple2(timestamp - startTimestamp, latency);
        }
    }

    private static class RecordLatencyHistogram {
        private Histogram histogram = new Histogram(5);

        String map(Tuple2<Long, Long> timestampAndLatency) {
            long timestamp = timestampAndLatency.f0();
            String timeMsg = String.format("%,d ", TOTAL_TIME_MILLIS - timestamp);
            if (histogram == null) {
                System.out.format("benchmarking is done -- %s", timeMsg);
                return null;
            }
            if (timestamp < WARMUP_TIME_MILLIS) {
                System.out.format("warming up -- %s", timeMsg);
            } else {
                System.out.print(timeMsg);
                histogram.recordValue(timestampAndLatency.f1());
            }
            if (timestamp >= TOTAL_TIME_MILLIS) {
                try {
                    return exportHistogram(histogram);
                } finally {
                    histogram = null;
                }
            }
            return null;
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static StreamSource<Long> longSource(long itemsPerSecond) {
        return SourceBuilder
                .timestampedStream("longs", c -> new LongSource(itemsPerSecond))
                .fillBufferFn(LongSource::fillBuffer)
                .build();
    }

    private static class LongSource {
        private static final long REPORT_PERIOD_NANOS = SECONDS.toNanos(SOURCE_THROUGHPUT_REPORTING_PERIOD_SECONDS);
        private static final long HICCUP_REPORT_THRESHOLD_MILLIS = 10;
        private final long nanoTimeMillisToCurrentTimeMillis = determineTimeOffset();
        private final long emitPeriod;
        private long counter;

        private long emitSchedule = System.nanoTime();
        private long lastReport = emitSchedule;
        private long counterAtLastReport;
        private long lastCallNanos = emitSchedule;

        LongSource(long itemsPerSecond) {
            emitPeriod = SECONDS.toNanos(1) / itemsPerSecond;
        }

        void fillBuffer(TimestampedSourceBuffer<Long> buf) {
            long nowNanos = System.nanoTime();
            long millisSinceLastCall = NANOSECONDS.toMillis(nowNanos - lastCallNanos);
            if (millisSinceLastCall > HICCUP_REPORT_THRESHOLD_MILLIS) {
                System.out.printf("*** Source hiccup: %,d ms%n", millisSinceLastCall);
            }
            lastCallNanos = nowNanos;
            long limit = counter + 256;
            for (;
                 emitSchedule <= nowNanos && counter < limit;
                 emitSchedule += emitPeriod, counter++
            ) {
                buf.add(counter, NANOSECONDS.toMillis(emitSchedule) - nanoTimeMillisToCurrentTimeMillis);
            }
            long nanosSinceLastReport = nowNanos - lastReport;
            if (nanosSinceLastReport < REPORT_PERIOD_NANOS) {
                return;
            }
            lastReport = nowNanos;
            long itemCountSinceLastReport = counter - counterAtLastReport;
            counterAtLastReport = counter;
            System.out.printf("%,d: %,.0f items/second%n",
                    simpleTime(NANOSECONDS.toMillis(nowNanos)),
                    itemCountSinceLastReport / ((double) nanosSinceLastReport / SECONDS.toNanos(1))
            );
        }
    }

    private static String exportHistogram(Histogram histogram) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bos);
        histogram.outputPercentileDistribution(out, 1.0);
        out.close();
        return bos.toString();
    }

    private static long determineTimeOffset() {
        long milliTime = System.currentTimeMillis();
        long nanoTime = System.nanoTime();
        return NANOSECONDS.toMillis(nanoTime) - milliTime;
    }

    private static long simpleTime(long timeMillis) {
        return timeMillis % SIMPLE_TIME_SPAN_MILLIS;
    }
}
