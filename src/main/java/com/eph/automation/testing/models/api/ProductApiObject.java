package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductApiObject {
    public ProductApiObject() {
    }

    private String productId;
    private String productName;
    private ProductIdentifiersApiObject[] productIdentifierApiObjects;
    private String productShortName;
    private Boolean separatelySaleableIndicator;
    private Boolean trialAllowedIndicator;
    private String launchDate;
    private HashMap<String,Object> productType;
    private HashMap<String,Object> productStatus;
    private PricesApiObject[] prices;
    private HashMap<String,Object> revenueModel;
    private HashMap<String,Object> etaxProductCode;
    public ProductManifestationApiObject manifestation;

    public ProductIdentifiersApiObject[] getProductIdentifierApiObjects() {
        return productIdentifierApiObjects;
    }

    public void setProductIdentifierApiObjects(ProductIdentifiersApiObject[] productIdentifierApiObjects) {
        this.productIdentifierApiObjects = productIdentifierApiObjects;
    }

    public PricesApiObject[] getPrices() {
        return prices;
    }

    public void setPrices(PricesApiObject[] prices) {
        this.prices = prices;
    }

    public ProductManifestationApiObject getManifestationApiObject() {
        return manifestation;
    }

    public void setManifestationApiObject(ProductManifestationApiObject manifestation) {
        this.manifestation = manifestation;
    }

    public HashMap<String, Object> getProductStatus() {
        return productStatus;
    }

    public void setProductStatus(HashMap<String, Object> productStatus) {
        this.productStatus = productStatus;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductShortName() {
        return productShortName;
    }

    public void setProductShortName(String productShortName) {
        this.productShortName = productShortName;
    }

    public Boolean getSeparatelySaleableIndicator() {
        return separatelySaleableIndicator;
    }

    public void setSeparatelySaleableIndicator(Boolean separatelySaleableIndicator) {
        this.separatelySaleableIndicator = separatelySaleableIndicator;
    }

    public Boolean getTrialAllowedIndicator() {
        return trialAllowedIndicator;
    }

    public void setTrialAllowedIndicator(Boolean trialAllowedIndicator) {
        this.trialAllowedIndicator = trialAllowedIndicator;
    }

    public String getLaunchDate() {
        return launchDate;
    }

    public void setLaunchDate(String launchDate) {
        this.launchDate = launchDate;
    }

    public HashMap<String, Object> getProductType() {
        return productType;
    }

    public void setProductType(HashMap<String, Object> productType) {
        this.productType = productType;
    }

    public HashMap<String, Object> getRevenueModel() {
        return revenueModel;
    }

    public void setRevenueModel(HashMap<String, Object> revenueModel) {
        this.revenueModel = revenueModel;
    }

}
