package com.trivadis.kafka.claimcheck.interceptor.metrics;

/**
 * JMX view of the {@link ConsumerInterceptorToStorage} behaviour. Registered under
 * {@code com.trivadis.kafka.claimcheck:type=ConsumerInterceptor,client-id=<client.id>}.
 */
public interface ConsumerInterceptorMetricsMBean {

    /** Number of records whose payload was fetched back from object storage. */
    long getRecordsMaterialized();

    /** Number of records passed through unchanged (not a reference). */
    long getRecordsPassedThrough();

    /** Total number of payload bytes read from object storage. */
    long getBytesFetched();

    /** Number of failures while fetching / deserializing a payload from object storage. */
    long getFetchErrors();
}
