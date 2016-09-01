package org.apache.ignite.tests.load.summit2016;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.common.SystemHelper;
import org.apache.ignite.tests.pojos.Product;
import org.apache.ignite.tests.pojos.ProductOrder;
import org.apache.ignite.tests.utils.TestsHelper;

import javax.cache.Cache;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class OrderWorker extends Worker {
    private static final int MIN = TestsHelper.getLoadTestsProductAbsMin();
    private static final int MAX = TestsHelper.getLoadTestsProductAbsMax();
    private static final String HOST_PREFIX;
    private static final int PRODUCTS_RANDOM_LOAD_COUNT = 10000;
    private static volatile boolean PRODUCTS_RANDOMLY_LOADED = false;

    private static final Map<Long, Product> products = new HashMap<>(100000);

    private static Long LAST_ADDED_PRODUCT_ID;

    static {
        String[] parts = SystemHelper.HOST_IP.split("\\.");
        if (parts[2].equals("0"))
            parts[2] = "777";

        if (parts[3].equals("0"))
            parts[3] = "777";

        HOST_PREFIX = parts[3] + parts[2];
    }

    private final Random RANDOM = new Random(System.currentTimeMillis());

    private IgniteCache igniteCache;

    public OrderWorker(Ignite ignite, long startPosition, long endPosition) {
        super(ignite, startPosition, endPosition);

        try {
            igniteCache = ignite.getOrCreateCache("product");
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to get instance of 'product' cache", e);
        }
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
    protected Object[] nextKeyValue() {
        int productId = MIN + RANDOM.nextInt(MAX - MIN + 1);
        productId = productId > MAX ? MAX : productId;

        Product prod = getProduct(productId);
        if (prod == null) {
            prod = getAnyProduct();

            if (prod == null)
                throw new IllegalStateException("Failed to get any product from Ignite 'product' cache");
        }

        Calendar cl = Calendar.getInstance();
        cl.set(Calendar.YEAR, TestsHelper.getLoadTestsYear());
        cl.set(Calendar.MONTH, TestsHelper.getLoadTestsMonth());
        cl.set(Calendar.DAY_OF_MONTH, TestsHelper.getLoadTestsDay());

        long orderId = Long.parseLong(HOST_PREFIX + (System.currentTimeMillis() + Thread.currentThread().getId()));

        ProductOrder order = TestsHelper.generateRandomOrder(prod, orderId, cl.getTime());

        return new Object[]{orderId, order};
    }

    private Product getProduct(long id) {
        synchronized (products) {
            Product prod = products.get(id);
            if (prod != null)
                return prod;
        }

        Product prod = (Product)igniteCache.get(id);

        if (prod != null) {
            synchronized (products) {
                LAST_ADDED_PRODUCT_ID = id;
                products.put(id, prod);
            }
        }

        return prod;
    }

    private Product getAnyProduct() {
        loadRandomProducts();

        synchronized (products) {
            return products.get(LAST_ADDED_PRODUCT_ID);
        }
    }

    private void loadRandomProducts() {
        if (PRODUCTS_RANDOMLY_LOADED)
            return;

        synchronized (products) {
            if (PRODUCTS_RANDOMLY_LOADED)
                return;

            int i = 0;

            for (Object entry : igniteCache) {
                products.put((Long)((Cache.Entry) entry).getKey(), (Product)((Cache.Entry) entry).getValue());
                LAST_ADDED_PRODUCT_ID = (Long)((Cache.Entry) entry).getKey();
                i++;

                if (i > PRODUCTS_RANDOM_LOAD_COUNT)
                    break;
            }

            PRODUCTS_RANDOMLY_LOADED = true;
        }
    }
}
