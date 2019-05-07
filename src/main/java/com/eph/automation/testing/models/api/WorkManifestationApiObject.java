package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
   public class WorkManifestationApiObject {
    public WorkManifestationApiObject() {
    }

    private String manifestationId;
    private String manifestationKeyTitle;
    private Boolean internationalEditionFlag;
    private String firstPubDate;

    public List<ManifestationIdentifiersApiObject> getManifestationIdentifiers() {
        return manifestationIdentifiers;
    }

    public void setManifestationIdentifiers(List<ManifestationIdentifiersApiObject> manifestationIdentifiers) {
        this.manifestationIdentifiers = manifestationIdentifiers;
    }

    private List<ManifestationIdentifiersApiObject> manifestationIdentifiers;
    private HashMap<String, Object> manifestationType;
    private HashMap<String, Object> manifestationStatus;

    public Products[] getProducts() {
        return products;
    }

    public void setProducts(Products[] products) {
        this.products = products;
    }

    private Products[] products;


    public String getManifestationId() {
        return manifestationId;
    }

    public void setManifestationId(String manifestationId) {
        this.manifestationId = manifestationId;
    }

    public String getManifestationKeyTitle() {
        return manifestationKeyTitle;
    }

    public void setManifestationKeyTitle(String manifestationKeyTitle) {
        this.manifestationKeyTitle = manifestationKeyTitle;
    }

    public Boolean getInternationalEditionFlag() {
        return internationalEditionFlag;
    }

    public void setInternationalEditionFlag(Boolean internationalEditionFlag) {
        this.internationalEditionFlag = internationalEditionFlag;
    }

    public String getFirstPubDate() {
        return firstPubDate;
    }

    public void setFirstPubDate(String firstPubDate) {
        this.firstPubDate = firstPubDate;
    }

    public HashMap<String, Object> getManifestationType() {
        return manifestationType;
    }

    public void setManifestationType(HashMap<String, Object> manifestationType) {
        this.manifestationType = manifestationType;
    }

    public HashMap<String, Object> getManifestationStatus() {
        return manifestationStatus;
    }

    public void setManifestationStatus(HashMap<String, Object> manifestationStatus) {
        this.manifestationStatus = manifestationStatus;
    }


    static class Products {
        public Products() {

        }

        public String getProductId() {
            return productId;
        }

        public void setProductId(String productId) {
            this.productId = productId;
        }

        String productId;
    }
}


