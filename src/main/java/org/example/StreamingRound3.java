package org.example;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import org.HdrHistogram.Histogram;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StreamingRound3 {
    private static final long EVENTS_PER_SECOND = 1_000_000;
    private static final long NUM_KEYS = 90_000;
    private static final long WIN_SIZE_MILLIS = 10_000;
    private static final long SLIDING_STEP_MILLIS = 10;

    private static final long INITIAL_SOURCE_DELAY_MILLIS = 10;
    private static final long LATENCY_REPORTING_THRESHOLD = 3;
    private static final long WARMUP_TIME_MILLIS = SECONDS.toMillis(20);
    private static final long MEASUREMENT_TIME_MILLIS = MINUTES.toMillis(4);
    private static final long TOTAL_TIME_MILLIS = WARMUP_TIME_MILLIS + MEASUREMENT_TIME_MILLIS;

    private static final long DIAGNOSTIC_KEYSET_DOWNSAMPLING_FACTOR = 1_000;
    private static final long SOURCE_THROUGHPUT_REPORTING_PERIOD_MILLIS = 10;
    static final int THROUGHPUT_REPORTING_THRESHOLD = 3_000_000;
    private static final long SIMPLE_TIME_SPAN_MILLIS = HOURS.toMillis(3);

    public static void main(String[] args) {
        System.out.printf(
                "%,d events per second%n" +
                "%,d keys%n" +
                "%,d milliseconds sliding window%n" +
                "%,d milliseconds sliding step%n" +
                "%,d milliseconds latency reporting threshold%n",
                EVENTS_PER_SECOND, NUM_KEYS, WIN_SIZE_MILLIS, SLIDING_STEP_MILLIS,
                LATENCY_REPORTING_THRESHOLD
        );
        Pipeline pipeline = buildPipeline();
        JetInstance jet = Jet.bootstrappedInstance();
        Job job = jet.newJob(pipeline);
        Runtime.getRuntime().addShutdownHook(new Thread(job::cancel));
        job.join();
    }

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        StreamStage<Long> source = p.readFrom(longSource(EVENTS_PER_SECOND, INITIAL_SOURCE_DELAY_MILLIS))
                                    .withNativeTimestamps(0);
        StreamStage<Tuple2<Long, Long>> latencies = source
                .rebalance()
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
            long time = simpleTime(timestamp);
            if (latency >= LATENCY_REPORTING_THRESHOLD) {
                System.out.format("time %,d: latency %,d ms, key %,d, count %,d%n",
                        time, latency, kwr.getKey(), count);
            }
            return tuple2(timestamp - startTimestamp, latency);
        }
    }

    private static class RecordLatencyHistogram {
        private Histogram histogram = new Histogram(5);

        String map(Tuple2<Long, Long> timestampAndLatency) {
            long timestamp = timestampAndLatency.f0();
            String timeMsg = String.format("%,d ", TOTAL_TIME_MILLIS - timestamp);
            if (histogram == null) {
                if (timestamp % 1_000 == 0) {
                    System.out.format("benchmarking is done -- %s%n", timeMsg);
                }
                return null;
            }
            if (timestamp < WARMUP_TIME_MILLIS) {
                if (timestamp % 2_000 == 0) {
                    System.out.format("warming up -- %s%n", timeMsg);
                }
            } else {
                if (timestamp % 10_000 == 0) {
                    System.out.println(timeMsg);
                }
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
    private static StreamSource<Long> longSource(long itemsPerSecond, long initialDelay) {
        return Sources.streamFromProcessorWithWatermarks("longs", true, eventTimePolicy -> ProcessorMetaSupplier.of(
                (Address ignored) -> {
                    long startTime = System.currentTimeMillis() + initialDelay;
                    return ProcessorSupplier.of(() ->
                            new LongSourceP(startTime, itemsPerSecond, eventTimePolicy, true));
                })
        );
    }

    private static class LongSourceP extends AbstractProcessor {
        private static final long THROUGHPUT_REPORT_PERIOD_NANOS =
                MILLISECONDS.toNanos(SOURCE_THROUGHPUT_REPORTING_PERIOD_MILLIS);
        private static final long HICCUP_REPORT_THRESHOLD_MILLIS = 10;
        private final long nanoTimeMillisToCurrentTimeMillis = determineTimeOffset();
        private final long startTime;
        private final long itemsPerSecond;
        private final boolean isReportingThroughput;
        private final long wmGranularity;
        private final long wmOffset;
        private long globalProcessorIndex;
        private long totalParallelism;
        private long emitPeriod;

        private final AppendableTraverser<Object> traverser = new AppendableTraverser<>(2);
        private long emitSchedule;
        private long lastReport;
        private long counterAtLastReport;
        private long lastCallNanos;
        private long counter;
        private long lastEmittedWm;
        private long nowNanos;

        public LongSourceP(
                long startTime,
                long itemsPerSecond,
                EventTimePolicy<? super Long> eventTimePolicy,
                boolean shouldReportThroughput
        ) {
            this.wmGranularity = eventTimePolicy.watermarkThrottlingFrameSize();
            this.wmOffset = eventTimePolicy.watermarkThrottlingFrameOffset();
            this.startTime = MILLISECONDS.toNanos(startTime + nanoTimeMillisToCurrentTimeMillis);
            this.itemsPerSecond = itemsPerSecond;
            this.isReportingThroughput = shouldReportThroughput;
        }

        @Override
        protected void init(Context context) {
            totalParallelism = context.totalParallelism();
            globalProcessorIndex = context.globalProcessorIndex();
            emitPeriod = SECONDS.toNanos(1) * totalParallelism / itemsPerSecond;
            lastCallNanos = lastReport = emitSchedule =
                    startTime + SECONDS.toNanos(1) * globalProcessorIndex / itemsPerSecond;
        }

        @Override
        public boolean complete() {
            nowNanos = System.nanoTime();
            emitEvents();
            detectAndReportHiccup();
            if (isReportingThroughput) {
                reportThroughput();
            }
            return false;
        }

        private void emitEvents() {
            while (emitFromTraverser(traverser) && emitSchedule <= nowNanos) {
                long timestamp = NANOSECONDS.toMillis(emitSchedule) - nanoTimeMillisToCurrentTimeMillis;
                traverser.append(jetEvent(timestamp, counter * totalParallelism + globalProcessorIndex));
                counter++;
                emitSchedule += emitPeriod;
                if (timestamp >= lastEmittedWm + wmGranularity) {
                    long wmToEmit = timestamp - (timestamp % wmGranularity) + wmOffset;
                    traverser.append(new Watermark(wmToEmit));
                    lastEmittedWm = wmToEmit;
                }
            }
        }

        private void detectAndReportHiccup() {
            long millisSinceLastCall = NANOSECONDS.toMillis(nowNanos - lastCallNanos);
            if (millisSinceLastCall > HICCUP_REPORT_THRESHOLD_MILLIS) {
                System.out.printf("*** Source #%d hiccup: %,d ms%n", globalProcessorIndex, millisSinceLastCall);
            }
            lastCallNanos = nowNanos;
        }

        private void reportThroughput() {
            long nanosSinceLastReport = nowNanos - lastReport;
            if (nanosSinceLastReport < THROUGHPUT_REPORT_PERIOD_NANOS) {
                return;
            }
            lastReport = nowNanos;
            long itemCountSinceLastReport = counter - counterAtLastReport;
            counterAtLastReport = counter;
            double throughput = itemCountSinceLastReport / ((double) nanosSinceLastReport / SECONDS.toNanos(1));
            if (throughput >= (double) THROUGHPUT_REPORTING_THRESHOLD / totalParallelism) {
                System.out.printf("%,d p%d: %,.0f items/second%n",
                        simpleTime(NANOSECONDS.toMillis(nowNanos)),
                        globalProcessorIndex,
                        throughput
                );
            }
        }

        @Override
        public boolean tryProcessWatermark(Watermark watermark) {
            throw new UnsupportedOperationException("Source processor shouldn't be asked to process a watermark");
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
