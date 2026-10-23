package com.trivadis.kafkaws.refbased.interceptor;

import java.util.concurrent.atomic.AtomicLong;

/** Thread-safe counters backing {@link ProducerInterceptorMetricsMBean}. */
class ProducerInterceptorMetrics implements ProducerInterceptorMetricsMBean {

    private final AtomicLong recordsExternalized = new AtomicLong();
    private final AtomicLong recordsInline = new AtomicLong();
    private final AtomicLong bytesExternalized = new AtomicLong();
    private final AtomicLong externalizationErrors = new AtomicLong();

    void recordExternalized(long bytes) {
        recordsExternalized.incrementAndGet();
        bytesExternalized.addAndGet(bytes);
    }

    void recordInline() {
        recordsInline.incrementAndGet();
    }

    void recordError() {
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
