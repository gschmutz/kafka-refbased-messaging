package com.trivadis.kafka.claimcheck.interceptor.metrics;

/**
 * JMX view of the {@link ProducerInterceptorToStorage} behaviour. Registered under
 * {@code com.trivadis.kafka.claimcheck:type=ProducerInterceptor,client-id=<client.id>}.
 */
public interface ProducerInterceptorMetricsMBean {

    /** Number of records whose payload was externalized to object storage. */
    long getRecordsExternalized();

    /** Number of records left inline on the topic (payload at or below the size threshold). */
    long getRecordsInline();

    /** Total number of payload bytes written to object storage. */
    long getBytesExternalized();

    /** Number of failures while writing a payload to object storage. */
    long getExternalizationErrors();
}
