package com.trivadis.kafkaws.refbased.interceptor.metrics;

import java.util.concurrent.atomic.AtomicLong;

/** Thread-safe counters backing {@link ConsumerInterceptorMetricsMBean}. */
public class ConsumerInterceptorMetrics implements ConsumerInterceptorMetricsMBean {

    private final AtomicLong recordsMaterialized = new AtomicLong();
    private final AtomicLong recordsPassedThrough = new AtomicLong();
    private final AtomicLong bytesFetched = new AtomicLong();
    private final AtomicLong fetchErrors = new AtomicLong();

    public void recordMaterialized(long bytes) {
        recordsMaterialized.incrementAndGet();
        bytesFetched.addAndGet(bytes);
    }

    public void recordPassThrough() {
        recordsPassedThrough.incrementAndGet();
    }

    public void recordError() {
        fetchErrors.incrementAndGet();
    }

    @Override
    public long getRecordsMaterialized() {
        return recordsMaterialized.get();
    }

    @Override
    public long getRecordsPassedThrough() {
        return recordsPassedThrough.get();
    }

    @Override
    public long getBytesFetched() {
        return bytesFetched.get();
    }

    @Override
    public long getFetchErrors() {
        return fetchErrors.get();
    }
}
