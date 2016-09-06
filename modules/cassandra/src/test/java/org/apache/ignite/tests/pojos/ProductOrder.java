package org.apache.ignite.tests.pojos;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ProductOrder {
    private static final DateFormat FORMAT = new SimpleDateFormat("MM/dd/yyyy/S");

    private long id;
    private long productId;
    private Date date;
    private int amount;
    private float price;

    public ProductOrder() {
    }

    public ProductOrder(Product product, long id, Date date, int amount) {
        this.id = id;
        this.productId = product.getId();
        this.date = date;
        this.amount = amount;
        this.price = product.getPrice() * amount;

        if (amount > 10)
            this.price = this.price * 0.95F;
    }

    public ProductOrder(long productId, float productPrice, long id, Date date, int amount) {
        this.id = id;
        this.productId = productId;
        this.date = date;
        this.amount = amount;
        this.price = productPrice * amount;

        if (amount > 10)
            this.price = this.price * 0.95F;
    }

    @Override
    public int hashCode() {
        return ((Long)id).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ProductOrder && id == ((ProductOrder) obj).id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @QuerySqlField(index = true)
    public long getId() {
        return id;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    @QuerySqlField(index = true)
    public long getProductId() {
        return productId;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @QuerySqlField
    public Date getDate() {
        return date;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    @QuerySqlField
    public int getAmount() {
        return amount;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    @QuerySqlField
    public float getPrice() {
        return price;
    }

    public void setDayMillisecond(String part) {
    }

    public String getDayMillisecond() {
        return FORMAT.format(date);
    }
}
