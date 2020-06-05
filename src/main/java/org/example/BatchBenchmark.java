package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;

import java.util.concurrent.TimeUnit;

public class BatchBenchmark {
    private static final long RANGE = 400_000_000;
    private static final long NUM_KEYS = 100_000_000;

    public static void main(String[] args) throws Exception {
        JetInstance jet = Jet.newJetInstance();
        Thread.sleep(20_000); // use this pause to connect with VisualVM
        Pipeline p = Pipeline.create();
        p.readFrom(longSource())
         .rebalance()
         .groupingKey(n -> n % NUM_KEYS)
         .aggregate(summingLongBoxed())
         .filter(e -> (e.getKey() & 0xFF_FFFFL) == 0)
         .writeTo(Sinks.logger())
        ;
        try {
            long start = System.nanoTime();
            jet.newJob(p).join();
            long took = System.nanoTime() - start;
            System.out.printf("Took %,d ms%n", TimeUnit.NANOSECONDS.toMillis(took));
        } finally {
            Jet.shutdownAll();
        }
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
                    for (int i = 0; i < 128 && n < RANGE; i++, n++) {
                        buf.add(n);
                        if ((n & 0xFF_FFFFL) == 0) {
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
