package org.apache.ignite.tests;

import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import javax.cache.configuration.Factory;

import java.lang.management.ManagementFactory;

public class TestCacheStoreSessionListener implements CacheStoreSessionListener, Factory<TestCacheStoreSessionListener> {
    private static volatile Integer COUNTER = 0;

    private String listenerName;

    public TestCacheStoreSessionListener() {
        synchronized (COUNTER) {
            listenerName = "listener_" + (++COUNTER);
        }
    }

    @Override
    public void onSessionStart(CacheStoreSession ses) {
        System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " [SESSION-START] [" + listenerName + "] " + ses.cacheName() + ", " + (ses.isWithinTransaction() ? "TRANSACTIONAL" : "ATOMIC"));
    }

    @Override
    public void onSessionEnd(CacheStoreSession ses, boolean commit) {
        System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " [SESSION-END] [" + listenerName + "] " + ses.cacheName() + ", " + (ses.isWithinTransaction() ? "TRANSACTIONAL" : "ATOMIC"));
    }

    @Override
    public TestCacheStoreSessionListener create() {
        return new TestCacheStoreSessionListener();
    }
}
