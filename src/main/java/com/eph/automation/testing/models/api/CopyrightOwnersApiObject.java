package com.eph.automation.testing.models.api;

import java.util.HashMap;

public class CopyrightOwnersApiObject {
    public CopyrightOwnersApiObject() {
    }
    public void compareWithDB(){

    }
    private HashMap<String, Object> copyrightOwner;
    private String effectiveStartDate;
    private String effectiveEndDate;
    private HashMap<String, Object> accessModel;

    public HashMap<String, Object> getCopyrightOwner() {
        return copyrightOwner;
    }

    public void setCopyrightOwner(HashMap<String, Object> copyrightOwner) {
        this.copyrightOwner = copyrightOwner;
    }

    public String getEffectiveStartDate() {
        return effectiveStartDate;
    }

    public void setEffectiveStartDate(String effectiveStartDate) {
        this.effectiveStartDate = effectiveStartDate;
    }

    public String getEffectiveEndDate() {
        return effectiveEndDate;
    }

    public void setEffectiveEndDate(String effectiveEndDate) {
        this.effectiveEndDate = effectiveEndDate;
    }

    public HashMap<String, Object> getAccessModel() {
        return accessModel;
    }

    public void setAccessModel(HashMap<String, Object> accessModel) {
        this.accessModel = accessModel;
    }

}
