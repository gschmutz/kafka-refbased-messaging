package com.trivadis.kafka.claimcheck.interceptor.metrics;

import java.lang.management.ManagementFactory;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Helper to (un)register the interceptor metrics MBeans on the platform MBean server. Registration
 * is best-effort: any JMX failure is swallowed so monitoring never breaks message flow.
 */
public final class JmxMetrics {

    static final String DOMAIN = "com.trivadis.kafka.claimcheck";

    private JmxMetrics() {
    }

    /**
     * Registers {@code mbean} under {@code <DOMAIN>:type=<type>,client-id=<clientId>}, replacing any
     * previously registered MBean with the same name.
     *
     * @return the registered {@link ObjectName}, or {@code null} if registration failed
     */
    public static ObjectName register(Object mbean, String type, String clientId) {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName(DOMAIN + ":type=" + type + ",client-id=" + sanitize(clientId));
            if (server.isRegistered(name)) {
                server.unregisterMBean(name);
            }
            server.registerMBean(mbean, name);
            return name;
        } catch (JMException e) {
            return null;
        }
    }

    public static void unregister(ObjectName name) {
        if (name == null) {
            return;
        }
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
        } catch (JMException ignored) {
            // best effort
        }
    }

    /** Replaces characters not allowed in an ObjectName value and falls back to "default". */
    private static String sanitize(String clientId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return "default";
        }
        return clientId.trim().replaceAll("[:\",=*?\\n]", "_");
    }
}
