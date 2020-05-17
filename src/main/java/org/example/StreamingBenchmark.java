package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.linearTrend;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StreamingBenchmark {
    private static final long EVENTS_PER_SECOND = 1_000_000;
    private static final long NUM_KEYS = 5_000_000;
    private static final long WIN_SIZE_MILLIS = 10_000;
    private static final long SLIDING_STEP_MILLIS = 1_000;

    private static final long INITIAL_DELAY_SECONDS = 0;
    private static final long DIAGNOSTIC_KEYSET_DOWNSAMPLING_FACTOR = 10_000;
    private static final long SOURCE_THROUGHPUT_REPORTING_PERIOD_SECONDS = 1;
    private static final long SIMPLE_TIME_SPAN = MINUTES.toSeconds(30);

    public static void main(String[] args) {
        Pipeline pipeline = buildPipeline();
        JetConfig cfg = new JetConfig();
        cfg.getDefaultEdgeConfig().setQueueSize(256);
        JetInstance jet = Jet.newJetInstance(cfg);
        try {
            jet.newJob(pipeline).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        StreamStage<Long> source = p.readFrom(longSource(EVENTS_PER_SECOND))
                                    .withNativeTimestamps(0)
                                    .rebalance();
        source.groupingKey(n -> n % NUM_KEYS)
              .window(sliding(WIN_SIZE_MILLIS, SLIDING_STEP_MILLIS))
              .aggregate(counting())
              .filter(kwr -> kwr.getKey() % DIAGNOSTIC_KEYSET_DOWNSAMPLING_FACTOR == 0)
              .window(tumbling(SLIDING_STEP_MILLIS))
              .aggregate(counting())
              .writeTo(Sinks.logger(wr -> String.format("time %,d: latency %,d ms, cca. %,d keys",
                      simpleTime(wr.end()),
                      NANOSECONDS.toMillis(System.nanoTime()) - wr.end(),
                      wr.result() * DIAGNOSTIC_KEYSET_DOWNSAMPLING_FACTOR)));
        return p;
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
        private static final long HICCUP_REPORT_THRESHOLD_MILLIS = 200;
        private final long emitPeriod;
        private long counter;
        private long emitSchedule = System.nanoTime() + SECONDS.toNanos(INITIAL_DELAY_SECONDS);
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
                buf.add(counter, NANOSECONDS.toMillis(emitSchedule));
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

    private static long simpleTime(long timeMillis) {
        return MILLISECONDS.toSeconds(timeMillis) % SIMPLE_TIME_SPAN;
    }
}
