package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;

public class ClusterBatchBenchmark {
    private static final long RANGE = 1_000_000_000;
    private static final int SOURCE_STEP = 10;
    private static final long NUM_KEYS = 500_000_000;

    public static void main(String[] args) throws Exception {
        Pipeline p = Pipeline.create();
        p.readFrom(longSource())
         .rebalance()
         .flatMap(n -> {
             Long[] items = new Long[SOURCE_STEP];
             Arrays.setAll(items, i -> n + i);
             return traverseArray(items);
         })
         .rebalance()
         .groupingKey(n -> n % NUM_KEYS)
         .aggregate(summingLongBoxed())
         .filter(e -> e.getKey() % 1_000_000 == 0)
         .writeTo(Sinks.logger())
        ;
        JetInstance jet = Jet.bootstrappedInstance();
        long start = System.nanoTime();
        Job job = jet.newJob(p);
        Runtime.getRuntime().addShutdownHook(new Thread(job::cancel));
        job.join();
        long took = System.nanoTime() - start;
        System.out.printf("Took %,d ms%n", TimeUnit.NANOSECONDS.toMillis(took));
    }

    private static AggregateOperation1<Long, MutableReference<Long>, Long> summingLongBoxed() {
        return AggregateOperation
                .withCreate(() -> new MutableReference<>(0L))
                .<Long>andAccumulate((acc, item) -> acc.set(acc.get() + item))
                .andCombine((acc, that) -> acc.set(acc.get() + that.get()))
                .andDeduct((acc, that) -> acc.set(acc.get() - that.get()))
                .andExportFinish(MutableReference::get);
    }

    private static BatchSource<Long> longSource() {
        return SourceBuilder
                .batch("longs", c -> new LongAccumulator())
                .<Long>fillBufferFn((counter, buf) -> {
                    long n = counter.get();
                    for (int i = 0; i < 128 && n < RANGE; i++, n += SOURCE_STEP) {
                        buf.add(n);
                        if ( n % (100_000 * SOURCE_STEP) == 0) {
                            System.out.printf("Emit %,d%n", n);
                        }
                    }
                    counter.set(n);
                    if (n == RANGE) {
                        buf.close();
                    }
                })
                .build();
    }
}

