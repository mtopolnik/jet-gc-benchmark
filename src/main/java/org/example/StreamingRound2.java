package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.datamodel.Tuple3;
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
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StreamingRound2 {
    private static final long EVENTS_PER_SECOND = 1_000_000;
    private static final long NUM_KEYS = 40_000;
    private static final long WIN_SIZE_MILLIS = 10_000;
    private static final long SLIDING_STEP_MILLIS = 100;

    private static final long WARMUP_TIME_MILLIS = SECONDS.toMillis(20);
    private static final long MEASUREMENT_TIME_MILLIS = MINUTES.toMillis(3);

    private static final long DIAGNOSTIC_KEYSET_DOWNSAMPLING_FACTOR = 10_000;
    private static final long SOURCE_THROUGHPUT_REPORTING_PERIOD_SECONDS = 1;
    private static final long SIMPLE_TIME_SPAN_SECONDS = 10_000;

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
        source.groupingKey(n -> n % NUM_KEYS)
              .window(sliding(WIN_SIZE_MILLIS, SLIDING_STEP_MILLIS))
              .aggregate(counting())
              .filter(kwr -> kwr.getKey() % DIAGNOSTIC_KEYSET_DOWNSAMPLING_FACTOR == 0)
              .window(tumbling(SLIDING_STEP_MILLIS))
              .aggregate(counting())
              .map(wr -> {
                  Tuple3<Long, Long, Long> t3 = tuple3(
                          wr.end(),
                          System.currentTimeMillis() - wr.end(),
                          wr.result() * DIAGNOSTIC_KEYSET_DOWNSAMPLING_FACTOR);
                  System.out.format("time %,d: latency %,d ms, cca. %,d keys%n",
                          simpleTime(t3.f0()),
                          t3.f1(),
                          t3.f2());
                  return t3;
              }).setLocalParallelism(1)
              .mapStateful(LongAccumulator::new, (timeToStart, t3) -> {
                  if (timeToStart.get() == 0) {
                      timeToStart.set(t3.f0() + WARMUP_TIME_MILLIS);
                  }
                  if (t3.f0() < timeToStart.get()) {
                      System.out.println("Warming up");
                      return null;
                  }
                  return t3;
              })
              .mapStateful(() -> new Object[] { new Histogram(5), null }, (state, t3) -> {
                  Histogram histogram = (Histogram) state[0];
                  if (histogram == null) {
                      System.out.println("Benchmarking is done");
                      return null;
                  }
                  if (state[1] == null) {
                      state[1] = t3.f0();
                  }
                  long start = (long) state[1];
                  if (t3.f0() < start + MEASUREMENT_TIME_MILLIS) {
                      histogram.recordValue(t3.f1());
                      return null;
                  }
                  try {
                      return exportHistogram(histogram);
                  } finally {
                      state[0] = null;
                  }
              })
              .filter(histogram -> {
                  System.out.println(histogram);
                  return true;
              })
              .writeTo(Sinks.files("/home/ec2-user"));
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
        long nanoTime = System.nanoTime();
        long milliTime = System.currentTimeMillis();
        return NANOSECONDS.toMillis(nanoTime) - milliTime;
    }

    private static long simpleTime(long timeMillis) {
        return MILLISECONDS.toSeconds(timeMillis) % SIMPLE_TIME_SPAN_SECONDS;
    }
}
