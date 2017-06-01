package org.apache.ignite.tests.store;

import javax.cache.configuration.Factory;

/**
 * Simple cache store factory class to test BinaryObjects functionality
 * @param <K> Ignite cache key type
 * @param <V> Ignite cache value type
 */
public class BinaryObjectsCacheStoreFactory<K, V> implements Factory<BinaryObjectsCacheStore<K, V>> {
    @Override
    public BinaryObjectsCacheStore<K, V> create() {
        return new BinaryObjectsCacheStore<>();
    }
}
