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

package org.apache.ignite.tests;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.ignite.transactions.Transaction;
import org.apache.log4j.Logger;

/**
 * Unit tests for Ignite caches which utilizing {@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}
 * to store cache data into Cassandra tables
 */
public class TransactionsTestClient {
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/primitive/transactions/ignite-client-config-1.xml")) {

            CacheConfiguration g;
            IgniteCache<Long, Long> longCache = ignite.getOrCreateCache(new CacheConfiguration<Long, Long>("cache1"));
            IgniteCache<String, String> strCache = ignite.getOrCreateCache(new CacheConfiguration<String, String>("cache2"));

            IgniteTransactions transactions = ignite.transactions();

            try (Transaction tx = transactions.txStart()) {
                for (int i = 0; i < 10000; i++) {
                    longCache.put((long)i, (long)i);
                    strCache.put(Integer.toString(i), Integer.toString(i));

                }

                tx.commit();
            }
            catch (Throwable e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        Ignition.stopAll(true);
    }
}
