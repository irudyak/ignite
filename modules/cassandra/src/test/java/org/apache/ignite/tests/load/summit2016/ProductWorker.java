package org.apache.ignite.tests.load.summit2016;

import org.apache.ignite.Ignite;
import org.apache.ignite.tests.pojos.Product;
import org.apache.ignite.tests.utils.TestsHelper;

public class ProductWorker extends Worker {
    private long position;
    private long endPosition;

    public ProductWorker(Ignite ignite, long startPosition, long endPosition) {
        super(ignite, startPosition, endPosition);
        this.position = startPosition;
        this.endPosition = endPosition;
    }

    @Override
    protected String cacheName() {
        return "product";
    }

    @Override
    protected String loggerName() {
        return "ProductsLoadTests";
    }

    @Override
    protected boolean continueExecution() {
        return position <= endPosition;
    }

    @Override
    protected Object[] nextKeyValue() {
        long id = position++;
        Product prod = TestsHelper.generateRandomProduct(id);

        log.info("New product: " + id);

        return new Object[]{id, prod};
    }
}
