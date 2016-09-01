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

package org.apache.ignite.tests.load.summit2016;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.cassandra.common.SystemHelper;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;

/**
 * Worker thread abstraction to be inherited by specific load test implementation
 */
public abstract class Worker extends Thread {
    /** */
    private static final SimpleDateFormat TIME_FORMATTER = new SimpleDateFormat("hh:mm:ss");

    /** */
    private long testStartTime;

    /** */
    boolean warmup = TestsHelper.getLoadTestsWarmupPeriod() != 0;

    /** */
    private volatile long warmupStartTime = 0;

    /** */
    private volatile long warmupFinishTime = 0;

    /** */
    private volatile long startTime = 0;

    /** */
    private volatile long finishTime = 0;

    /** */
    private volatile long warmupMsgProcessed = 0;

    /** */
    private volatile long warmupSleepCnt = 0;

    /** */
    private volatile long msgProcessed = 0;

    /** */
    private volatile long msgFailed = 0;

    /** */
    private volatile long sleepCnt = 0;

    /** */
    private Throwable executionError;

    /** */
    private long statReportedTime;

    /** */
    private Ignite ignite;

    /** */
    private IgniteCache igniteCache;

    /** */
    private Logger log;

    /** */
    public Worker(Ignite ignite, long startPosition, long endPosition) {
        this.ignite = ignite;
        this.log = Logger.getLogger(loggerName());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void run() {
        try {
            if (ignite != null)
                igniteCache = ignite.getOrCreateCache(cacheName());

            execute();
        }
        catch (Throwable e) {
            executionError = e;
            throw new RuntimeException("Test execution abnormally terminated", e);
        }
        finally {
            reportTestCompletion();
        }
    }

    /** */
    public boolean isFailed() {
        return executionError != null;
    }

    /** */
    public long getSpeed() {
        if (msgProcessed == 0)
            return 0;

        long finish = finishTime != 0 ? finishTime : System.currentTimeMillis();
        long duration = (finish - startTime - sleepCnt * TestsHelper.getLoadTestsRequestsLatency()) / 1000;

        return duration == 0 ? msgProcessed : msgProcessed / duration;
    }

    /** */
    public long getErrorsCount() {
        return msgFailed;
    }

    /** */
    public float getErrorsPercent() {
        if (msgFailed == 0)
            return 0;

        return msgProcessed + msgFailed == 0 ? 0 : (float)(msgFailed * 100 ) / (float)(msgProcessed + msgFailed);
    }

    /** */
    public long getMsgCountTotal() {
        return warmupMsgProcessed + msgProcessed;
    }

    /** */
    public long getWarmupMsgProcessed() {
        return warmupMsgProcessed;
    }

    /** */
    public long getMsgProcessed() {
        return msgProcessed;
    }

    protected abstract String cacheName();

    protected abstract String loggerName();

    protected abstract Object[] nextKeyValue();

    protected boolean continueExecution() {
        return System.currentTimeMillis() - testStartTime <
                TestsHelper.getLoadTestsWarmupPeriod() + TestsHelper.getLoadTestsExecutionTime();
    }

    /** */
    @SuppressWarnings("unchecked")
    private void execute() throws InterruptedException {
        testStartTime = System.currentTimeMillis();

        log.info("Test execution started");

        if (warmup)
            log.info("Warm up period started");

        warmupStartTime = warmup ? testStartTime : 0;
        startTime = !warmup ? testStartTime : 0;

        statReportedTime = testStartTime;

        try {
            while (continueExecution()) {
                if (warmup && System.currentTimeMillis() - testStartTime > TestsHelper.getLoadTestsWarmupPeriod()) {
                    warmupFinishTime = System.currentTimeMillis();
                    startTime = warmupFinishTime;
                    statReportedTime = warmupFinishTime;
                    warmup = false;
                    log.info("Warm up period completed");
                }

                Object[] keyVal = nextKeyValue();

                try {
                    igniteCache.put(keyVal[0], keyVal[1]);
                    updateMetrics(1);
                }
                catch (Throwable e) {
                    log.error("Failed to perform single operation", e);
                    updateErrorMetrics(1);
                }

                reportStatistics();
            }
        }
        finally {
            warmupFinishTime = warmupFinishTime != 0 ? warmupFinishTime : System.currentTimeMillis();
            finishTime = System.currentTimeMillis();
        }
    }

    /** */
    public long getWarmUpSpeed() {
        if (warmupMsgProcessed == 0)
            return 0;

        long finish = warmupFinishTime != 0 ? warmupFinishTime : System.currentTimeMillis();
        long duration = (finish - warmupStartTime - warmupSleepCnt * TestsHelper.getLoadTestsRequestsLatency()) / 1000;

        return duration == 0 ? warmupMsgProcessed : warmupMsgProcessed / duration;
    }

    /** */
    private void updateMetrics(int itemsProcessed) {
        if (warmup)
            warmupMsgProcessed += itemsProcessed;
        else
            msgProcessed += itemsProcessed;

        if (TestsHelper.getLoadTestsRequestsLatency() > 0) {
            try {
                Thread.sleep(TestsHelper.getLoadTestsRequestsLatency());

                if (warmup)
                    warmupSleepCnt++;
                else
                    sleepCnt++;
            }
            catch (Throwable ignored) {
            }
        }
    }

    /**
     * TODO IGNITE-1371 Comment absent.
     *
     * @param itemsFailed Failed item.
     */
    private void updateErrorMetrics(int itemsFailed) {
        if (!warmup)
            msgFailed += itemsFailed;
    }

    /** */
    private void reportStatistics() {
        // statistics should be reported only every 30 seconds
        if (System.currentTimeMillis() - statReportedTime < 30000)
            return;

        statReportedTime = System.currentTimeMillis();

        int completed = warmup ?
                (int)(statReportedTime - warmupStartTime) * 100 / TestsHelper.getLoadTestsWarmupPeriod() :
                (int)(statReportedTime - startTime) * 100 / TestsHelper.getLoadTestsExecutionTime();

        if (completed > 100)
            completed = 100;

        if (warmup) {
            log.info("Warm up messages processed " + warmupMsgProcessed + ", " +
                "speed " + getWarmUpSpeed() + " msg/sec, " + completed + "% completed");
        }
        else {
            log.info("Messages processed " + msgProcessed + ", " +
                "speed " + getSpeed() + " msg/sec, " + completed + "% completed, " +
                "errors " + msgFailed + " / " + String.format("%.2f", getErrorsPercent()).replace(",", ".") + "%");
        }
    }

    /** */
    private void reportTestCompletion() {
        StringBuilder builder = new StringBuilder();

        if (executionError != null)
            builder.append("Test execution abnormally terminated. ");
        else
            builder.append("Test execution successfully completed. ");

        builder.append("Statistics: ").append(SystemHelper.LINE_SEPARATOR);
        builder.append("Start time: ").append(TIME_FORMATTER.format(testStartTime)).append(SystemHelper.LINE_SEPARATOR);
        builder.append("Finish time: ").append(TIME_FORMATTER.format(finishTime)).append(SystemHelper.LINE_SEPARATOR);
        builder.append("Duration: ").append((finishTime - testStartTime) / 1000).append(" sec")
            .append(SystemHelper.LINE_SEPARATOR);

        if (TestsHelper.getLoadTestsWarmupPeriod() > 0) {
            builder.append("Warm up period: ").append(TestsHelper.getLoadTestsWarmupPeriod() / 1000)
                .append(" sec").append(SystemHelper.LINE_SEPARATOR);
            builder.append("Warm up processed messages: ").append(warmupMsgProcessed).append(SystemHelper.LINE_SEPARATOR);
            builder.append("Warm up processing speed: ").append(getWarmUpSpeed())
                .append(" msg/sec").append(SystemHelper.LINE_SEPARATOR);
        }

        builder.append("Processed messages: ").append(msgProcessed).append(SystemHelper.LINE_SEPARATOR);
        builder.append("Processing speed: ").append(getSpeed()).append(" msg/sec").append(SystemHelper.LINE_SEPARATOR);
        builder.append("Errors: ").append(msgFailed).append(" / ").
                append(String.format("%.2f", getErrorsPercent()).replace(",", ".")).append("%");

        if (executionError != null)
            log.error(builder.toString(), executionError);
        else
            log.info(builder.toString());
    }
}
