package org.apache.ignite.tests.pojos;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Order {
    private static final DateFormat FORMAT = new SimpleDateFormat("MM_dd_yyyy_HH");

    private long id;
    private long productId;
    private Date date;
    private int amount;
    private float price;

    public Order() {
    }

    public Order(Product product, long id, Date date, int amount) {
        this.id = id;
        this.productId = product.getId();
        this.date = date;
        this.amount = amount;
        this.price = product.getPrice() * amount;

        if (amount > 10)
            this.price = this.price * 0.95F;
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

    @QuerySqlField(index = true)
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

    public void setOrderHour(String hour) {
    }

    public String getOrderHour() {
        return FORMAT.format(date);
    }
}
