package com.clickhouse.kafka.connect.util.jmx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public final class MBeanServerUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(MBeanServerUtils.class);

    private MBeanServerUtils() {

    }

    public static <T> T registerMBean(final T mBean, final String mBeanName) {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            server.registerMBean(mBean, new ObjectName(mBeanName));
            return mBean;
        } catch (InstanceAlreadyExistsException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            // JMX might not be available
            LOGGER.warn("Unable to register MBean " + mBeanName, e);
            return mBean;
        }
    }
    public static void unregisterMBean(final String mBeanName) {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName objectName = new ObjectName(mBeanName);
            if (server.isRegistered(objectName)) {
                server.unregisterMBean(objectName);
            }
        } catch (Exception e) {
            // JMX might not be available
            LOGGER.warn("Unable to unregister MBean " + mBeanName, e);
        }
    }
}
