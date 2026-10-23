package com.trivadis.kafkaws.refbased.interceptor;

import java.util.concurrent.atomic.AtomicLong;

/** Thread-safe counters backing {@link ConsumerInterceptorMetricsMBean}. */
class ConsumerInterceptorMetrics implements ConsumerInterceptorMetricsMBean {

    private final AtomicLong recordsMaterialized = new AtomicLong();
    private final AtomicLong recordsPassedThrough = new AtomicLong();
    private final AtomicLong bytesFetched = new AtomicLong();
    private final AtomicLong fetchErrors = new AtomicLong();

    void recordMaterialized(long bytes) {
        recordsMaterialized.incrementAndGet();
        bytesFetched.addAndGet(bytes);
    }

    void recordPassThrough() {
        recordsPassedThrough.incrementAndGet();
    }

    void recordError() {
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
