package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
class FinancialAttributesApiObject {

    public FinancialAttributesApiObject() {
    }
    public void compareWithDB(){

    }
    private HashMap<String, Object> glCompany;

    public HashMap<String, Object> getGlCompany() {
        return glCompany;
    }

    public void setGlCompany(HashMap<String, Object> glCompany) {
        this.glCompany = glCompany;
    }

    public HashMap<String, Object> getCostResponsibilityCentre() {
        return costResponsibilityCentre;
    }

    public void setCostResponsibilityCentre(HashMap<String, Object> costResponsibilityCentre) {
        this.costResponsibilityCentre = costResponsibilityCentre;
    }

    public HashMap<String, Object> getRevenueResponsibilityCentre() {
        return revenueResponsibilityCentre;
    }

    public void setRevenueResponsibilityCentre(HashMap<String, Object> revenueResponsibilityCentre) {
        this.revenueResponsibilityCentre = revenueResponsibilityCentre;
    }

    public String getEffectiveStartDate() {
        return effectiveStartDate;
    }

    public void setEffectiveStartDate(String effectiveStartDate) {
        this.effectiveStartDate = effectiveStartDate;
    }

    private HashMap<String, Object> costResponsibilityCentre;
    private HashMap<String, Object> revenueResponsibilityCentre;
    private String effectiveStartDate;

}