package com.eph.automation.testing.models.api;

import java.util.HashMap;

/*
Created by Nishant @ 27 Nov 2019
* */
public class ManifestationProductAPIObject {

    public ManifestationProductAPIObject() {
    }

    private ProductSummary productSummary;
    public ProductSummary getProductSummary() {
        return productSummary;
    }

    public void setId(ProductSummary productSummary) {
        this.productSummary = productSummary;
    }

    static class ProductSummary {
        public ProductSummary() {
        }

        String id;
        String name;
        HashMap<String, Object> type;
        HashMap<String, Object> status;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public HashMap<String, Object> getType() {
            return type;
        }

        public void setType(HashMap<String, Object> type) {
            this.type = type;
        }

        public HashMap<String, Object> getStatus() {
            return status;
        }

        public void setStatus(HashMap status) {
            this.status = status;
        }

    }

}
