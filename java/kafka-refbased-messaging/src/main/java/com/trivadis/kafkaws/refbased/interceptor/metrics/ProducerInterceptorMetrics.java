package com.trivadis.kafkaws.refbased.interceptor.metrics;

import java.util.concurrent.atomic.AtomicLong;

/** Thread-safe counters backing {@link ProducerInterceptorMetricsMBean}. */
public class ProducerInterceptorMetrics implements ProducerInterceptorMetricsMBean {

    private final AtomicLong recordsExternalized = new AtomicLong();
    private final AtomicLong recordsInline = new AtomicLong();
    private final AtomicLong bytesExternalized = new AtomicLong();
    private final AtomicLong externalizationErrors = new AtomicLong();

    public void recordExternalized(long bytes) {
        recordsExternalized.incrementAndGet();
        bytesExternalized.addAndGet(bytes);
    }

    public void recordInline() {
        recordsInline.incrementAndGet();
    }

    public void recordError() {
        externalizationErrors.incrementAndGet();
    }

    @Override
    public long getRecordsExternalized() {
        return recordsExternalized.get();
    }

    @Override
    public long getRecordsInline() {
        return recordsInline.get();
    }

    @Override
    public long getBytesExternalized() {
        return bytesExternalized.get();
    }

    @Override
    public long getExternalizationErrors() {
        return externalizationErrors.get();
    }
}
