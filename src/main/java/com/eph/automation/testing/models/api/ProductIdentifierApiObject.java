package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
class ProductIdentifierApiObject {
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