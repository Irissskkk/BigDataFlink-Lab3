package com.flink;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class SaleEvent implements Serializable {
    @JsonProperty("transaction_id")
    public String transactionId;
    
    @JsonProperty("product_id")
    public int productId;
    
    @JsonProperty("product_name")
    public String productName;
    
    @JsonProperty("category")
    public String category;
    
    @JsonProperty("customer_id")
    public int customerId;
    
    @JsonProperty("customer_name")
    public String customerName;
    
    @JsonProperty("city")
    public String city;
    
    @JsonProperty("store_id")
    public int storeId;
    
    @JsonProperty("sale_amount")
    public double saleAmount;
    
    @JsonProperty("sale_timestamp")
    public String saleTimestamp;
    
    // Конструктор по умолчанию (нужен для Flink)
    public SaleEvent() {}
    
    public SaleEvent(String transactionId, int productId, String productName, 
                     String category, int customerId, String customerName, 
                     String city, int storeId, double saleAmount, String saleTimestamp) {
        this.transactionId = transactionId;
        this.productId = productId;
        this.productName = productName;
        this.category = category;
        this.customerId = customerId;
        this.customerName = customerName;
        this.city = city;
        this.storeId = storeId;
        this.saleAmount = saleAmount;
        this.saleTimestamp = saleTimestamp;
    }
    
    @Override
    public String toString() {
        return String.format("SaleEvent{transactionId='%s', productId=%d, productName='%s', " +
                           "category='%s', customerId=%d, customerName='%s', city='%s', " +
                           "storeId=%d, saleAmount=%.2f, saleTimestamp='%s'}",
                           transactionId, productId, productName, category,
                           customerId, customerName, city, storeId, saleAmount, saleTimestamp);
    }
}