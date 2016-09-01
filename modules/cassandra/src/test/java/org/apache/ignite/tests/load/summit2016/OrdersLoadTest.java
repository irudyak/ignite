package org.apache.ignite.tests.load.summit2016;

import org.apache.ignite.Ignite;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.log4j.Logger;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class OrdersLoadTest extends LoadTestDriver {
    private static final Logger LOGGER = Logger.getLogger("OrdersLoadTests");

    public static void main(String[] args) {
        try {
            LOGGER.info("Start loading orders");

            LoadTestDriver driver = new OrdersLoadTest();

            driver.runTest();

            LOGGER.info("Orders loading completed");
        }
        catch (Throwable e) {
            LOGGER.error("Orders loading failed", e);
            throw new RuntimeException("Orders loading failed", e);
        }
    }

    @Override
    protected String testName() {
        return "ORDERS_LOAD";
    }

    @Override
    protected long startPosition() {
        return 0;
    }

    @Override
    protected long endPosition() {
        return 0;
    }

    @Override
    protected Logger logger() {
        return LOGGER;
    }

    @Override
    protected Worker createWorker(Ignite ignite, long startPosition, long endPosition) {
        return new OrderWorker(ignite, startPosition, endPosition);
    }
}
