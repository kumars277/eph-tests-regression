package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductApiObject {
    public ProductApiObject() {
    }

    private String id;
    private String name;
    private String shortName;
    private Boolean separatelySaleableInd;
    private Boolean trialAllowedInd;
    private String launchDate;
    private HashMap<String,Object> type;
    private HashMap<String,Object> status;
    private PricesApiObject[] prices;
    private HashMap<String,Object> revenueModel;
    private HashMap<String,Object> etaxProductCode;
    private WorkPersonsApiObject[] persons;
    private WorkApiObject work;

    public ProductManifestationApiObject manifestation;
    private ProductIdentifiersApiObject[] productIdentifierApiObjects;

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

    public HashMap<String, Object> getStatus() {
        return status;
    }

    public void setStatus(HashMap<String, Object> status) {
        this.status = status;
    }

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

    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public Boolean getSeparatelySaleableInd() {
        return separatelySaleableInd;
    }

    public void setSeparatelySaleableInd(Boolean separatelySaleableInd) {
        this.separatelySaleableInd = separatelySaleableInd;
    }

    public Boolean getTrialAllowedInd() {
        return trialAllowedInd;
    }

    public void setTrialAllowedInd(Boolean trialAllowedInd) {
        this.trialAllowedInd = trialAllowedInd;
    }

    public String getLaunchDate() {
        return launchDate;
    }

    public void setLaunchDate(String launchDate) {
        this.launchDate = launchDate;
    }

    public HashMap<String, Object> getType() {
        return type;
    }

    public void setType(HashMap<String, Object> type) {
        this.type = type;
    }

    public HashMap<String, Object> getRevenueModel() {
        return revenueModel;
    }

    public void setRevenueModel(HashMap<String, Object> revenueModel) {
        this.revenueModel = revenueModel;
    }

}
