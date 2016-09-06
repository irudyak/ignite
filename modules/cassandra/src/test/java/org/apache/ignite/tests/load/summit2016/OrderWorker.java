package org.apache.ignite.tests.load.summit2016;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.store.cassandra.common.SystemHelper;
import org.apache.ignite.tests.pojos.ProductOrder;
import org.apache.ignite.tests.utils.TestsHelper;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class OrderWorker extends Worker {
    private static final int MIN = TestsHelper.getLoadTestsProductAbsMin();
    private static final int MAX = TestsHelper.getLoadTestsProductAbsMax();
    private static final String HOST_PREFIX;
    private static final int BATCH_SIZE = TestsHelper.getBulkOperationSize();

    static {
        String[] parts = SystemHelper.HOST_IP.split("\\.");

        String prefix = parts[3];
        prefix = prefix.length() > 2 ? prefix.substring(prefix.length() - 2) : prefix;

        HOST_PREFIX = prefix;
    }

    private final Random RANDOM = new Random(System.currentTimeMillis());
    private final int workerNumber;
    private Map batch = new HashMap(BATCH_SIZE);

    public OrderWorker(Ignite ignite, long startPosition, long endPosition, int workerNumber) {
        super(ignite, startPosition, endPosition);
        this.workerNumber = workerNumber;
    }

    @Override
    protected String cacheName() {
        return "order";
    }

    @Override
    protected String loggerName() {
        return "OrdersLoadTests";
    }

    @Override
    protected boolean batchMode() {
        return true;
    }

    @Override
    protected Object[] nextKeyValue() {
        ProductOrder order = nextOrder();

        //log.info("New order: " + order.getId() + " / " + order.getProductId());

        return new Object[]{order.getId(), order};
    }

    @Override
    protected Map nextKeyValueBatch() {
        batch.clear();

        for (int i = 0; i < BATCH_SIZE; i++) {
            ProductOrder order = nextOrder();
            batch.put(order.getId(), order);
        }

        return batch;
    }

    private ProductOrder nextOrder() {
        int productId = MIN + RANDOM.nextInt(MAX - MIN + 1);
        productId = productId > MAX ? MAX : productId;

        Calendar cl = Calendar.getInstance();
        cl.set(Calendar.YEAR, TestsHelper.getLoadTestsYear());
        cl.set(Calendar.MONTH, TestsHelper.getLoadTestsMonth());
        cl.set(Calendar.DAY_OF_MONTH, TestsHelper.getLoadTestsDay());

        long orderId = Long.parseLong(System.currentTimeMillis() + HOST_PREFIX + workerNumber);

        ProductOrder order = TestsHelper.generateRandomOrder(productId, orderId, cl.getTime());

        //log.info("New order: " + orderId + " / " + productId);

        return order;
    }
}
