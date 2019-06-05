package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
class ProductIdentifiersApiObject {
    private List<ProductIdentifierApiObject> productIdentifiers;

    public ProductIdentifiersApiObject() {
    }

    public List<ProductIdentifierApiObject> getProductIdentifiers() {
        return this.productIdentifiers;
    }

    public void setProductIdentifiers(List<ProductIdentifierApiObject> productIdentifiers) {
        this.productIdentifiers = productIdentifiers;
    }

}