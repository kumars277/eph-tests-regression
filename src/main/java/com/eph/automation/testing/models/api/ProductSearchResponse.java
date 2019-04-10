package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductSearchResponse {
    public ProductSearchResponse() {
    }

    private String productId;
    private String productName;
    private ProductIdentifiers[] productIdentifiers;
    private String productShortName;
    private Boolean separatelySaleableIndicator;
    private Boolean trialAllowedIndicator;
    private String launchDate;
    private HashMap<String,Object> productType;
    private HashMap<String,Object> productStatus;
    private Prices[] prices;
    private HashMap<String,Object> revenueModel;
    public Manifestation manifestation;

    public ProductIdentifiers[] getProductIdentifiers() {
        return productIdentifiers;
    }

    public void setProductIdentifiers(ProductIdentifiers[] productIdentifiers) {
        this.productIdentifiers = productIdentifiers;
    }

    public Prices[] getPrices() {
        return prices;
    }

    public void setPrices(Prices[] prices) {
        this.prices = prices;
    }

    public Manifestation getManifestation() {
        return manifestation;
    }

    public void setManifestation(Manifestation manifestation) {
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
