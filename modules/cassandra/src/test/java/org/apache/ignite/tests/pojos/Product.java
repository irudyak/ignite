package org.apache.ignite.tests.pojos;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Product {
    private long id;
    private String type;
    private String title;
    private String description;
    private float price;

    public Product() {
    }

    public Product(long id, String type, String title, String description, float price) {
        this.id = id;
        this.type = type;
        this.title = title;
        this.description = description;
        this.price = price;
    }

    @Override
    public String toString() {
        return id + ", " + price + ", " + type + ", " + title + ", " + description;
    }

    public void setId(long id) {
        this.id = id;
    }

    @QuerySqlField(index = true)
    public long getId() {
        return id;
    }

    public void setType(String type) {
        this.type = type;
    }

    @QuerySqlField
    public String getType() {
        return type;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @QuerySqlField(index = true)
    public String getTitle() {
        return title;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @QuerySqlField
    public String getDescription() {
        return description;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    @QuerySqlField
    public float getPrice() {
        return price;
    }
}
