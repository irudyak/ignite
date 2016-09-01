package org.apache.ignite.tests.load.summit2016;

import org.apache.ignite.Ignite;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.log4j.Logger;

public class ProductsLoadTest extends LoadTestDriver {
    private static final Logger LOGGER = Logger.getLogger("ProductsLoadTests");

    private static final int MIN = TestsHelper.getLoadTestsProductMin();
    private static final int MAX = TestsHelper.getLoadTestsProductMax();
    private static final int DELTA = (MAX - MIN) / TestsHelper.getLoadTestsThreadsCount();

    private int iteration = 0;

    public static void main(String[] args) {
        try {
            LOGGER.info("Start loading products");

            LoadTestDriver driver = new ProductsLoadTest();

            driver.runTest();

            LOGGER.info("Products loading completed");
        }
        catch (Throwable e) {
            LOGGER.error("Products loading failed", e);
            throw new RuntimeException("Products loading failed", e);
        }
    }

    @Override
    protected String testName() {
        return "PRODUCTS_LOAD";
    }

    @Override
    protected long startPosition() {
        long pos = MIN + iteration * DELTA;
        return pos < MAX ? pos : MAX - DELTA;
    }

    @Override
    protected long endPosition() {
        int pos = (int)startPosition() + DELTA - 1;
        ++iteration;
        return pos <= MAX ? pos : MAX;
    }

    @Override
    protected Logger logger() {
        return LOGGER;
    }

    @Override
    protected Worker createWorker(Ignite ignite, long startPosition, long endPosition) {
        return new ProductWorker(ignite, startPosition, endPosition);
    }
}
