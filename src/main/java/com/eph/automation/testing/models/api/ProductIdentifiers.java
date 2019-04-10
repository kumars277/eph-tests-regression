package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
class ProductIdentifiers {
    private List<ProductIdentifier> productIdentifiers;

    public ProductIdentifiers() {
    }

    public List<ProductIdentifier> getProductIdentifiers() {
        return this.productIdentifiers;
    }

    public void setProductIdentifiers(List<ProductIdentifier> productIdentifiers) {
        this.productIdentifiers = productIdentifiers;
    }


    private class ProductIdentifier {
        public ProductIdentifier() {
        }

        private String productIdentifier;
        private HashMap<String, Object> identifierType;

        public String getProductId() {
            return productIdentifier;
        }

        public void setProductId(String productIdentifier) {
            this.productIdentifier = productIdentifier;
        }

        public HashMap<String, Object> getIdentifierType() {
            return identifierType;
        }

        public void setIdentifierType(HashMap<String, Object> identifierType) {
            this.identifierType = identifierType;
        }
    }
}