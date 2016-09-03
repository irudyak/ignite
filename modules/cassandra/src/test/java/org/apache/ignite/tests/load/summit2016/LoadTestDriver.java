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
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.cassandra.common.SystemHelper;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * Basic load test driver to be inherited by specific implementation for particular use-case.
 */
public abstract class LoadTestDriver {
    /** Number of attempts to setup load test */
    private static final int NUMBER_OF_SETUP_ATTEMPTS = 10;

    /** Timeout between load test setup attempts */
    private static final int SETUP_ATTEMPT_TIMEOUT = 1000;

    /** */
    public void runTest() {
        logger().info("Running " + testName() + " test");

        Ignite ignite = null;

        int attempt;

        logger().info("Setting up load tests driver");

        for (attempt = 0; attempt < NUMBER_OF_SETUP_ATTEMPTS; attempt++) {
            try {
                ignite = Ignition.start(TestsHelper.getLoadTestsIgniteConfig());
                break;
            }
            catch (Throwable e) {
                logger().error((attempt + 1) + " attempt to setup load test '" + testName() + "' failed", e);
            }

            if (attempt + 1 != NUMBER_OF_SETUP_ATTEMPTS) {
                logger().info("Sleeping for " + SETUP_ATTEMPT_TIMEOUT + " seconds before trying next attempt " +
                        "to setup '" + testName() + "' load test");

                try {
                    Thread.sleep(SETUP_ATTEMPT_TIMEOUT);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }
            }
        }

        if (ignite == null && attempt == NUMBER_OF_SETUP_ATTEMPTS) {
            throw new RuntimeException("All " + NUMBER_OF_SETUP_ATTEMPTS + " attempts to setup load test '" +
                    testName() + "' have failed");
        }

        logger().info("Load tests driver setup successfully completed");

        try {

            List<Worker> workers = new LinkedList<>();

            logger().info("Starting workers");

            for (int i = 0; i < TestsHelper.getLoadTestsThreadsCount(); i++) {
                Worker worker = createWorker(ignite, startPosition(), endPosition(), i);
                workers.add(worker);
                worker.setName(testName() + "-worker-" + i);
                worker.start();
            }

            logger().info("Workers started");
            logger().info("Waiting for workers to complete");

            List<String> failedWorkers = new LinkedList<>();

            for (Worker worker : workers) {
                boolean failed = false;

                try {
                    worker.join();
                }
                catch (Throwable e) {
                    logger().error("Worker " + worker.getName() + " waiting interrupted", e);
                    failed = true;
                }

                if (failed || worker.isFailed()) {
                    failedWorkers.add(worker.getName());
                    logger().info("Worker " + worker.getName() + " execution failed");
                }
                else
                    logger().info("Worker " + worker.getName() + " successfully completed");
            }

            printTestResultsHeader(testName(), failedWorkers);
            printTestResultsStatistics(testName(), workers);
        }
        finally {
            if (ignite != null)
                ignite.close();
        }
    }

    /** */
    protected abstract String testName();

    /** */
    protected abstract long startPosition();

    /** */
    protected abstract long endPosition();

    /** */
    protected abstract Logger logger();

    /** */
    protected abstract Worker createWorker(Ignite ignite, long startPosition, long endPosition, int workerNumber);

    /** */
    private void printTestResultsHeader(String testName, List<String> failedWorkers) {
        if (failedWorkers.isEmpty()) {
            logger().info(testName + " test execution successfully completed.");
            return;
        }

        if (failedWorkers.size() == TestsHelper.getLoadTestsThreadsCount()) {
            logger().error(testName + " test execution totally failed.");
            return;
        }

        String strFailedWorkers = "";

        for (String workerName : failedWorkers) {
            if (!strFailedWorkers.isEmpty())
                strFailedWorkers += ", ";

            strFailedWorkers += workerName;
        }

        logger().warn(testName + " test execution completed, but " + failedWorkers.size() + " of " +
            TestsHelper.getLoadTestsThreadsCount() + " workers failed. Failed workers: " + strFailedWorkers);
    }

    /** */
    @SuppressWarnings("StringBufferReplaceableByString")
    private void printTestResultsStatistics(String testName, List<Worker> workers) {
        long cnt = 0;
        long errCnt = 0;
        long speed = 0;

        for (Worker worker : workers) {
            cnt += worker.getMsgCountTotal();
            errCnt += worker.getErrorsCount();
            speed += worker.getSpeed() != 0 ? worker.getSpeed() : worker.getWarmUpSpeed();
        }

        float errPercent = errCnt == 0 ?
            0 :
            cnt + errCnt ==  0 ? 0 : (float)(errCnt * 100 ) / (float)(cnt + errCnt);

        StringBuilder builder = new StringBuilder();
        builder.append(SystemHelper.LINE_SEPARATOR);
        builder.append("-------------------------------------------------");
        builder.append(SystemHelper.LINE_SEPARATOR);
        builder.append(testName).append(" test statistics").append(SystemHelper.LINE_SEPARATOR);
        builder.append(testName).append(" messages: ").append(cnt).append(SystemHelper.LINE_SEPARATOR);
        builder.append(testName).append(" errors: ").append(errCnt).append(", ").
                append(String.format("%.2f", errPercent).replace(",", ".")).
                append("%").append(SystemHelper.LINE_SEPARATOR);
        builder.append(testName).append(" speed: ").append(speed).append(" msg/sec").append(SystemHelper.LINE_SEPARATOR);
        builder.append("-------------------------------------------------");

        logger().info(builder.toString());
    }
}