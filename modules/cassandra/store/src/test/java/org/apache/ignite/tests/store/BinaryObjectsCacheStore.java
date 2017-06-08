package org.apache.ignite.tests.store;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.util.Collection;
import java.util.Map;

/**
 * Simple cache store class to test BinaryObjects functionality
 * @param <K> Ignite cache key type
 * @param <V> Ignite cache value type
 */
public class BinaryObjectsCacheStore<K, V> implements CacheStore<K, V> {
    @SuppressWarnings("unused")
    @IgniteInstanceResource
    private Ignite ignite;

    @SuppressWarnings("unused")
    @CacheStoreSessionResource
    private CacheStoreSession storeSes;

    @Override
    public void loadCache(IgniteBiInClosure<K, V> clo, @Nullable Object... args) throws CacheLoaderException {
    }

    @Override
    public void sessionEnd(boolean commit) throws CacheWriterException {
    }

    @Override
    public V load(K k) throws CacheLoaderException {
        return null;
    }

    @Override
    public Map<K, V> loadAll(Iterable<? extends K> iterable) throws CacheLoaderException {
        return null;
    }

    @Override
    public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        if (!(entry.getKey() instanceof BinaryObject))
            throw new RuntimeException("Entry key is not an instance of BinaryObject");

        if (!(entry.getValue() instanceof BinaryObject))
            throw new RuntimeException("Entry key is not an instance of BinaryObject");
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> collection) throws CacheWriterException {
        for (Cache.Entry<? extends K, ? extends V> entry: collection) {
            if (!(entry.getKey() instanceof BinaryObject))
                throw new RuntimeException("Entry key is not an instance of BinaryObject");

            if (!(entry.getValue() instanceof BinaryObject))
                throw new RuntimeException("Entry key is not an instance of BinaryObject");
        }
    }

    @Override
    public void delete(Object o) throws CacheWriterException {

    }

    @Override
    public void deleteAll(Collection<?> collection) throws CacheWriterException {

    }
}
