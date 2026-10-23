package com.trivadis.kafkaws.refbased.interceptor;

import java.lang.reflect.Field;

/**
 * Small reflection helpers for the interceptor tests. The interceptors build their S3 client
 * internally (no dependency injection), so the tests inject a mock and inspect parsed configuration
 * via the declared fields.
 */
final class TestSupport {

    private TestSupport() {
    }

    static void setField(Object target, String name, Object value) {
        try {
            Field f = target.getClass().getDeclaredField(name);
            f.setAccessible(true);
            f.set(target, value);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    static Object getField(Object target, String name) {
        try {
            Field f = target.getClass().getDeclaredField(name);
            f.setAccessible(true);
            return f.get(target);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
