/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.store.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.cassandra.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.cache.store.cassandra.persistence.PersistenceController;
import org.apache.ignite.cache.store.cassandra.session.CassandraSession;
import org.apache.ignite.cache.store.cassandra.session.ExecutionAssistant;
import org.apache.ignite.cache.store.cassandra.session.GenericBatchExecutionAssistant;
import org.apache.ignite.cache.store.cassandra.session.LoadCacheCustomQueryWorker;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.resources.LoggerResource;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Implementation of {@link CacheStore} backed by Cassandra database.
 *
 * @param <K> Ignite cache key type.
 * @param <V> Ignite cache value type.
 */
public class CassandraCacheStore1<K, V> implements CacheStore<K, V> {
    /** Connection attribute property name. */
    private static final String ATTR_CONN_PROP = "CASSANDRA_STORE_CONNECTION";

    /** Auto-injected store session. */
    @CacheStoreSessionResource
    private CacheStoreSession storeSes;

    /** Auto-injected logger instance. */
    @LoggerResource
    private IgniteLogger log;

    private static volatile Integer COUNTER = 0;

    private String storeName;

    /**
     * Store constructor.
     *
     */
    public CassandraCacheStore1() {
        synchronized (COUNTER) {
            storeName = "store_" + (++COUNTER);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<K, V> clo, Object... args) throws CacheLoaderException {
        System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " [LOAD-CACHE] [" + storeName + "]" + storeSes.cacheName());
    }

    /** {@inheritDoc} */
    @Override public void sessionEnd(boolean commit) throws CacheWriterException {
        System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " [SESSION-END] [" + storeName + "]" + storeSes.cacheName() + ", " + (commit ? "COMMIT" : "ROLLBACK"));
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public V load(final K key) throws CacheLoaderException {
        System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " [LOAD] [" + storeName + "]" + storeSes.cacheName() + ", " + key);
        return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Map<K, V> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
        for (Object key : keys)
            System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " [LOAD-ALL] [" + storeName + "]" + storeSes.cacheName() + ", " + key);

        return new HashMap<>();
    }

    /** {@inheritDoc} */
    @Override public void write(final Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        if (entry == null || entry.getKey() == null)
            return;

        System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " [WRITE] [" + storeName + "]" + storeSes.cacheName() + ", " + entry.getKey() + "=" + entry.getValue());
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) throws CacheWriterException {
        if (entries == null || entries.isEmpty())
            return;

        for (Cache.Entry entry : entries)
            System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " [WRITE-ALL] [" + storeName + "]" + storeSes.cacheName() + ", " + entry.getKey() + "=" + entry.getValue());
    }

    /** {@inheritDoc} */
    @Override public void delete(final Object key) throws CacheWriterException {
        System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " [DELETE] [" + storeName + "]" + storeSes.cacheName() + ", " + key);
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
        for (Object key : keys)
            System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " [DELETE-ALL] [" + storeName + "]" + storeSes.cacheName() + ", " + key);
    }
}
