/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridCacheRebalancingSyncSelfTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    private static int TEST_SIZE = 100_000;

    /** partitioned cache name. */
    protected static String CACHE_NAME_DHT_PARTITIONED = "cacheP";

    /** partitioned cache 2 name. */
    protected static String CACHE_NAME_DHT_PARTITIONED_2 = "cacheP2";

    /** replicated cache name. */
    protected static String CACHE_NAME_DHT_REPLICATED = "cacheR";

    /** replicated cache 2 name. */
    protected static String CACHE_NAME_DHT_REPLICATED_2 = "cacheR2";

    /** */
    private volatile boolean concurrentStartFinished;

    /** */
    private volatile boolean concurrentStartFinished2;

    /** */
    private volatile boolean concurrentStartFinished3;

    private static long testTimeout = 5 * 60_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(gridName);

        iCfg.setRebalanceThreadPoolSize(4);

        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setForceServerMode(true);

        if (getTestGridName(10).equals(gridName))
            iCfg.setClientMode(true);

        CacheConfiguration<Integer, Integer> cachePCfg = new CacheConfiguration<>();

        cachePCfg.setName(CACHE_NAME_DHT_PARTITIONED);
        cachePCfg.setCacheMode(CacheMode.PARTITIONED);
        cachePCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cachePCfg.setBackups(1);
        cachePCfg.setRebalanceBatchSize(1);
        cachePCfg.setRebalanceBatchesCount(1);
        cachePCfg.setRebalanceOrder(2);

        CacheConfiguration<Integer, Integer> cachePCfg2 = new CacheConfiguration<>();

        cachePCfg2.setName(CACHE_NAME_DHT_PARTITIONED_2);
        cachePCfg2.setCacheMode(CacheMode.PARTITIONED);
        cachePCfg2.setRebalanceMode(CacheRebalanceMode.SYNC);
        cachePCfg2.setBackups(1);
        cachePCfg2.setRebalanceOrder(2);
        //cachePCfg2.setRebalanceDelay(5000);//Known issue, deadlock in case of low priority rebalancing delay.

        CacheConfiguration<Integer, Integer> cacheRCfg = new CacheConfiguration<>();

        cacheRCfg.setName(CACHE_NAME_DHT_REPLICATED);
        cacheRCfg.setCacheMode(CacheMode.REPLICATED);
        cacheRCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheRCfg.setRebalanceBatchSize(1);
        cacheRCfg.setRebalanceBatchesCount(Integer.MAX_VALUE);

        CacheConfiguration<Integer, Integer> cacheRCfg2 = new CacheConfiguration<>();

        cacheRCfg2.setName(CACHE_NAME_DHT_REPLICATED_2);
        cacheRCfg2.setCacheMode(CacheMode.REPLICATED);
        cacheRCfg2.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheRCfg2.setRebalanceOrder(2);

        iCfg.setCacheConfiguration(cachePCfg, cachePCfg2, cacheRCfg, cacheRCfg2);
        iCfg.setSystemThreadPoolSize(128); // Shmem blocking fix.

        return iCfg;
    }

    /**
     * @param ignite Ignite.
     */
    protected void generateData(Ignite ignite, int from, int iter) {
        generateData(ignite, CACHE_NAME_DHT_PARTITIONED, from, iter);
        generateData(ignite, CACHE_NAME_DHT_PARTITIONED_2, from, iter);
        generateData(ignite, CACHE_NAME_DHT_REPLICATED, from, iter);
        generateData(ignite, CACHE_NAME_DHT_REPLICATED_2, from, iter);
    }

    /**
     * @param ignite Ignite.
     */
    protected void generateData(Ignite ignite, String name, int from, int iter) {
        for (int i = from; i < from + TEST_SIZE; i++) {
            if (i % (TEST_SIZE / 10) == 0)
                log.info("Prepared " + i * 100 / (TEST_SIZE) + "% entries (" + TEST_SIZE + ").");

            ignite.cache(name).put(i, i + name.hashCode() + iter);
        }
    }

    /**
     * @param ignite Ignite.
     * @throws IgniteCheckedException Exception.
     */
    protected void checkData(Ignite ignite, int from, int iter) throws IgniteCheckedException {
        checkData(ignite, CACHE_NAME_DHT_PARTITIONED, from, iter);
        checkData(ignite, CACHE_NAME_DHT_PARTITIONED_2, from, iter);
        checkData(ignite, CACHE_NAME_DHT_REPLICATED, from, iter);
        checkData(ignite, CACHE_NAME_DHT_REPLICATED_2, from, iter);
    }

    /**
     * @param ignite Ignite.
     * @param name Cache name.
     * @throws IgniteCheckedException Exception.
     */
    protected void checkData(Ignite ignite, String name, int from, int iter) throws IgniteCheckedException {
        for (int i = from; i < from + TEST_SIZE; i++) {
            if (i % (TEST_SIZE / 10) == 0)
                log.info("<" + name + "> Checked " + i * 100 / (TEST_SIZE) + "% entries (" + TEST_SIZE + ").");

            assert ignite.cache(name).get(i) != null && ignite.cache(name).get(i).equals(i + name.hashCode() + iter) :
                i + " value " + (i + name.hashCode() + iter) + " does not match (" + ignite.cache(name).get(i) + ")";
        }
    }

    /**
     * @throws Exception Exception
     */
    public void testSimpleRebalancing() throws Exception {
        Ignite ignite = startGrid(0);

        generateData(ignite, 0, 0);

        log.info("Preloading started.");

        long start = System.currentTimeMillis();

        startGrid(1);

        waitForRebalancing(0, 2);
        waitForRebalancing(1, 2);

        stopGrid(0);

        waitForRebalancing(1, 3);

        startGrid(2);

        waitForRebalancing(1, 4);
        waitForRebalancing(2, 4);

        stopGrid(2);

        waitForRebalancing(1, 5);

        long spend = (System.currentTimeMillis() - start) / 1000;

        checkData(grid(1), 0, 0);

        log.info("Spend " + spend + " seconds to rebalance entries.");

        stopAllGrids();
    }

    /**
     * @throws Exception Exception
     */
    public void testLoadRebalancing() throws Exception {
        final Ignite ignite = startGrid(0);

        startGrid(1);

        generateData(ignite, CACHE_NAME_DHT_PARTITIONED, 0, 0);

        log.info("Preloading started.");

        long start = System.currentTimeMillis();

        concurrentStartFinished = false;

        Thread t1 = new Thread() {
            @Override public void run() {
                while (!concurrentStartFinished) {
                    for (int i = 0; i < 0 + TEST_SIZE; i++) {
                        if (i % (TEST_SIZE / 10) == 0)
                            log.info("Prepared " + i * 100 / (TEST_SIZE) + "% entries (" + TEST_SIZE + ").");

                        ignite.cache(CACHE_NAME_DHT_PARTITIONED).put(i, i + CACHE_NAME_DHT_PARTITIONED.hashCode() + 0);
                    }
                }
            }
        };
        Thread t2 = new Thread() {
            @Override public void run() {
                while (!concurrentStartFinished) {
                    try {
                        checkData(ignite, CACHE_NAME_DHT_PARTITIONED, 0, 0);
                    }
                    catch (IgniteCheckedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        t1.start();
        t2.start();

        startGrid(2);

        waitForRebalancing(2, 3);

        concurrentStartFinished = true;

        t1.join();
        t2.join();

        long spend = (System.currentTimeMillis() - start) / 1000;

        log.info("Spend " + spend + " seconds to rebalance entries.");

        stopAllGrids();
    }

    /**
     * @param id Node id.
     * @param major Major ver.
     * @param minor Minor ver.
     * @throws IgniteCheckedException Exception.
     */
    protected void waitForRebalancing(int id, int major, int minor) throws IgniteCheckedException {
        waitForRebalancing(id, new AffinityTopologyVersion(major, minor));
    }

    /**
     * @param id Node id.
     * @param major Major ver.
     * @throws IgniteCheckedException Exception.
     */
    protected void waitForRebalancing(int id, int major) throws IgniteCheckedException {
        waitForRebalancing(id, new AffinityTopologyVersion(major));
    }

    /**
     * @param id Node id.
     * @param top Topology version.
     * @throws IgniteCheckedException
     */
    protected void waitForRebalancing(int id, AffinityTopologyVersion top) throws IgniteCheckedException {
        boolean finished = false;

        while (!finished) {
            finished = true;

            for (GridCacheAdapter c : grid(id).context().cache().internalCaches()) {
                GridDhtPartitionDemander.SyncFuture fut = (GridDhtPartitionDemander.SyncFuture)c.preloader().syncFuture();
                if (fut.topologyVersion() == null || !fut.topologyVersion().equals(top)) {
                    finished = false;

                    break;
                }
                else if (!fut.get()) {
                    finished = false;

                    log.warning("Rebalancing finished with missed partitions.");
                }
            }
        }
    }

//    static{
//        testTimeout = Integer.MAX_VALUE;
//    }
//    public void test() throws Exception {
//        while (true) {
//            testComplexRebalancing();
//
//            U.sleep(5000);
//
//            System.gc();
//
//        }
//    }

    @Override protected long getTestTimeout() {
        return testTimeout;
    }

    /**
     * @throws Exception
     */
    public void testComplexRebalancing() throws Exception {
        final Ignite ignite = startGrid(0);

        generateData(ignite, 0, 0);

        log.info("Preloading started.");

        long start = System.currentTimeMillis();

        concurrentStartFinished = false;
        concurrentStartFinished2 = false;
        concurrentStartFinished3 = false;

        Thread t1 = new Thread() {
            @Override public void run() {
                try {
                    startGrid(1);
                    startGrid(2);

                    while (!concurrentStartFinished2) {
                        U.sleep(10);
                    }

                    waitForRebalancing(0, 5, 0);
                    waitForRebalancing(1, 5, 0);
                    waitForRebalancing(2, 5, 0);
                    waitForRebalancing(3, 5, 0);
                    waitForRebalancing(4, 5, 0);

                    awaitPartitionMapExchange();

                    //New cache should start rebalancing.
                    CacheConfiguration<Integer, Integer> cacheRCfg = new CacheConfiguration<>();

                    cacheRCfg.setName(CACHE_NAME_DHT_PARTITIONED + "_NEW");
                    cacheRCfg.setCacheMode(CacheMode.PARTITIONED);
                    cacheRCfg.setRebalanceMode(CacheRebalanceMode.SYNC);

                    grid(0).getOrCreateCache(cacheRCfg);

                    while (!concurrentStartFinished3) {
                        U.sleep(10);
                    }

                    concurrentStartFinished = true;
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        Thread t2 = new Thread() {
            @Override public void run() {
                try {
                    startGrid(3);
                    startGrid(4);

                    concurrentStartFinished2 = true;
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        Thread t3 = new Thread() {
            @Override public void run() {
                generateData(ignite, 0, 1);

                concurrentStartFinished3 = true;
            }
        };

        t1.start();
        t2.start();// Should cancel t1 rebalancing.
        t3.start();

        t1.join();
        t2.join();
        t3.join();

        waitForRebalancing(0, 5, 1);
        waitForRebalancing(1, 5, 1);
        waitForRebalancing(2, 5, 1);
        waitForRebalancing(3, 5, 1);
        waitForRebalancing(4, 5, 1);

        checkData(grid(4), 0, 1);

        final Ignite ignite3 = grid(3);

        Thread t4 = new Thread() {
            @Override public void run() {
                generateData(ignite3, 0, 2);

            }
        };

        t4.start();

        stopGrid(0);

        waitForRebalancing(1, 6);
        waitForRebalancing(2, 6);
        waitForRebalancing(3, 6);
        waitForRebalancing(4, 6);

        stopGrid(1);

        waitForRebalancing(2, 7);
        waitForRebalancing(3, 7);
        waitForRebalancing(4, 7);

        stopGrid(2);

        waitForRebalancing(3, 8);
        waitForRebalancing(4, 8);

        t4.join();

        stopGrid(3);

        waitForRebalancing(4, 9);

        long spend = (System.currentTimeMillis() - start) / 1000;

        checkData(grid(4), 0, 2);

        log.info("Spend " + spend + " seconds to rebalance entries.");

        stopAllGrids();
    }

    /**
     * @throws Exception Exception.
     */
    public void testBackwardCompatibility() throws Exception {
        Ignite ignite = startGrid(0);

        Map<String, Object> map = new HashMap<>(ignite.cluster().localNode().attributes());

        map.put(IgniteNodeAttributes.REBALANCING_VERSION, 0);

        ((TcpDiscoveryNode)ignite.cluster().localNode()).setAttributes(map);

        generateData(ignite, 0, 0);

        startGrid(1);

        waitForRebalancing(1, 2);

        stopGrid(0);

        checkData(grid(1), 0, 0);

        stopAllGrids();
    }

    /**
     * @throws Exception Exception.
     */
    public void testNodeFailedAtRebalancing() throws Exception {
        Ignite ignite = startGrid(0);

        generateData(ignite, 0, 0);

        log.info("Preloading started.");

        startGrid(1);

        waitForRebalancing(1, 2);

        startGrid(2);

        waitForRebalancing(2, 3);

        ((TestTcpDiscoverySpi)grid(2).configuration().getDiscoverySpi()).simulateNodeFailure();

        waitForRebalancing(0, 4);
        waitForRebalancing(1, 4);

        stopAllGrids();
    }
}